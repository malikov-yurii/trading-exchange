package trading.exchange.orderserver;

import com.aitusoftware.babl.config.BablConfig;
import com.aitusoftware.babl.config.PerformanceMode;
import com.aitusoftware.babl.user.Application;
import com.aitusoftware.babl.user.ContentType;
import com.aitusoftware.babl.websocket.BablServer;
import com.aitusoftware.babl.websocket.DisconnectReason;
import com.aitusoftware.babl.websocket.SendResult;
import com.aitusoftware.babl.websocket.Session;
import com.aitusoftware.babl.websocket.SessionContainers;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderMessage;
import trading.api.OrderMessageSerDe;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.exchange.LeadershipManager;
import trading.exchange.LeadershipStateProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static trading.common.Utils.env;

public class OrderServer implements Application {
    private static final Logger log = LoggerFactory.getLogger(OrderServer.class);

    private final LFQueue<OrderRequest> clientRequests;
    private final LeadershipManager leadershipManager;

    private ReplicationConsumer replicationConsumer;

    private RequestSequencer requestSequencer;

    private final AtomicLong respSeqNum = new AtomicLong(1);

    private final Map<Long, Session> sessionsByClientId = new ConcurrentHashMap<>();
    private final Map<Long, Long> clientIdBySessionId = new ConcurrentHashMap<>();

    private SessionContainers containers;
    private final String bindAddress;
    private final int listenPort;

    private Thread replicationConsumerThread;

    private Thread wsServerThread;
    private Thread requestSequencerThread;
    private ShutdownSignalBarrier wsServerShutdownSignalBarrier;

    public OrderServer(LFQueue<OrderRequest> clientRequests,
                       LFQueue<OrderMessage> responseQueue,
                       LeadershipManager leadershipManager) {
        this.clientRequests = clientRequests;
        this.leadershipManager = leadershipManager;

        this.bindAddress = Utils.env("WS_IP", "0.0.0.0");
        this.listenPort = Integer.valueOf(Utils.env("WS_PORT", "8080"));
        responseQueue.subscribe(this::processResponse);
    }

    public synchronized void start() {
        leadershipManager.onLeadershipAcquired(() -> {
            stopReplicationConsumer();

            startRequestSequencer();
            startWsServer();
        });

        leadershipManager.onLeadershipLost(() -> {
            stopWsServer();
            stopRequestSequencer();

            startReplicationConsumer();
        });
    }

    private synchronized void startWsServer() {
        final BablConfig config = new BablConfig();
        config.sessionContainerConfig().bindAddress(bindAddress);
        config.sessionContainerConfig().listenPort(listenPort);
        config.applicationConfig().application(this);
        PerformanceMode perfMode = PerformanceMode.valueOf(env("BABL_PERFORMANCE_MODE", "DEVELOPMENT"));
        config.performanceConfig().performanceMode(perfMode);

        wsServerThread = new Thread(() -> {
            try {
                containers = BablServer.launch(config);
                containers.start();
                log.info("BablOrderServer started. PerformanceMode=[{}]. [{}:{}]", perfMode, bindAddress, listenPort);
                wsServerShutdownSignalBarrier = new ShutdownSignalBarrier();
                wsServerShutdownSignalBarrier.await();
            } catch (Exception e) {
                log.error("Error in BablOrderServer thread", e);
            } finally {
                if (containers != null) {
                    containers.close();
                }
                log.info("BablOrderServer thread has exited.");
            }
        }, "BablOrderServerThread");

        wsServerThread.start();
    }

    private synchronized void stopWsServer() {
        log.info("stopWsServer. Started");
        if (wsServerShutdownSignalBarrier != null) {
            wsServerShutdownSignalBarrier.signal();
        }
        if (wsServerThread != null && wsServerThread.isAlive()) {
            wsServerThread.interrupt();
            try {
                wsServerThread.join(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("stopWsServer. Done");
    }

    private synchronized void startReplicationConsumer() {
//        log.info("startReplicationConsumer. Started");
        replicationConsumer = new ReplicationConsumer(clientRequests);
        replicationConsumerThread = new Thread(replicationConsumer);
        replicationConsumerThread.start();
//        log.info("startReplicationConsumer. Done");
    }

    private synchronized void stopReplicationConsumer() {
        log.info("stopReplicationConsumer. Started");
        if (replicationConsumer != null) {
            replicationConsumer.shutdown();
        }
        if (replicationConsumerThread != null && replicationConsumerThread.isAlive()) {
            replicationConsumerThread.interrupt();
            try {
                replicationConsumerThread.join(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("stopReplicationConsumer. Done");
    }

    private synchronized void startRequestSequencer() {
//        log.info("startRequestSequencer. Started");
        requestSequencer = new RequestSequencer(clientRequests);
        requestSequencerThread = new Thread(requestSequencer);
        requestSequencerThread.start();
//        log.info("startRequestSequencer. Done");
    }

    private synchronized void stopRequestSequencer() {
        log.info("stopRequestSequencer. Started");
        if (requestSequencer != null) {
            requestSequencer.shutdown();
        }
        if (requestSequencerThread != null && requestSequencerThread.isAlive()) {
            requestSequencerThread.interrupt();
            try {
                requestSequencerThread.join(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("stopRequestSequencer. Done");
    }

    public synchronized void shutdown() {
        log.info("shutdown. Started");
        stopWsServer();
        stopRequestSequencer();
        log.info("shutdown. Done");
    }

    @Override
    public int onSessionConnected(final Session session) {
        log.info("Session {} connected", session.id());
        return SendResult.OK;
    }

    @Override
    public int onSessionDisconnected(Session session, DisconnectReason reason) {
        log.info("Session {} disconnected due to {}", session.id(), reason.name());
        Long clientId = clientIdBySessionId.remove(session.id());
        if (clientId != null) {
            sessionsByClientId.remove(clientId);
        }
        return SendResult.OK;
    }

    @Override
    public int onSessionMessage(Session session, ContentType contentType,
                                DirectBuffer msg, int offset, int length) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Received {} bytes from session {}", length, session != null ? session.id() : null);
            }

            OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(msg, offset, length);

            if (!sessionsByClientId.containsKey(orderRequest.getClientId())) {
                log.info("First request from clientId={}", orderRequest.getClientId());
                clientIdBySessionId.put(session.id(), orderRequest.getClientId());
                sessionsByClientId.put(orderRequest.getClientId(), session);
            }

            requestSequencer.process(orderRequest);
        } catch (Exception e) {
            log.error("Error processing message from session {}: {}", session.id(), e.getMessage(), e);
        }
        return SendResult.OK;
    }

    private void processResponse(OrderMessage orderMessage) {
        if (leadershipManager.isFollower()) {
//            if (log.isDebugEnabled()) {
//                log.debug("Not Publishing {}", orderMessage);
//            }
            log.info("Not Publishing {}", orderMessage);
            return;
        }
        if (orderMessage == null) {
            log.error("processResponse. Received null response");
            return;
        }

        long seq = respSeqNum.getAndIncrement();
        orderMessage.setSeqNum(seq);

        long clientId = orderMessage.getClientId();
        Session session = sessionsByClientId.get(clientId);
        if (session == null) {
            log.error("processResponse. Client session not found for clientId={}", clientId);
            return;
        }

        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
        int length = OrderMessageSerDe.serialize(orderMessage, buffer, 0);

        sendResponse(orderMessage, session, buffer, length);

        log.info("Sent response {}. OK", orderMessage);
    }

    private static void sendResponse(OrderMessage orderMessage, Session session, DirectBuffer buffer, int length) {
        int sendResult;
        int attempt = 0;
        do {
            sendResult = session.send(ContentType.BINARY, buffer, 0, length);
            if (sendResult != 0) {
                try {
                    if (log.isDebugEnabled()) {
                        log.info("Failed to send response {}. Retrying in {}ms...", orderMessage, attempt);
                    }
                    Thread.sleep(++attempt); // Pause for a moment before retrying
                } catch (InterruptedException e) {
                    log.error("Error while trying to pause between retries", e);
                    Thread.currentThread().interrupt();
                }
            }
        } while (sendResult != 0);
    }

}