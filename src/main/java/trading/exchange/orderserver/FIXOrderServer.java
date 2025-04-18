package trading.exchange.orderserver;

import fix.PipeDelimitedScreenLogFactory;
import fix.ReusableMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Acceptor;
import quickfix.Application;
import quickfix.ApplicationAdapter;
import quickfix.FieldNotFound;
import quickfix.FileStoreFactory;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketAcceptor;
import quickfix.UnsupportedMessageType;
import trading.api.OrderMessage;
import trading.api.OrderMessageSerDe;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.AsyncLogger;
import trading.common.LFQueue;
import trading.exchange.AppState;
import trading.exchange.LeadershipManager;
import trading.exchange.ReplayReplicationLogConsumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FIXOrderServer implements OrderServer {

    private static final Logger log = LoggerFactory.getLogger(FIXOrderServer.class);

    private final LFQueue<OrderRequest> clientRequests;
    private final LFQueue<OrderMessage> clientResponses;
    private final LeadershipManager leadershipManager;
    private final AppState appState;

    private ReplicationConsumer replicationConsumer;
    private RequestSequencer requestSequencer;
    private Thread requestSequencerThread;

    private final AtomicLong respSeqNum = new AtomicLong(1);
    private final Map<Long, SessionHolder> sessionsByClientId = new ConcurrentHashMap<>();
    record SessionHolder(SessionID sessionId, long clientId, Message reusableOutMessage) {}

    private long seqNum;
    private Acceptor acceptor;
    private final AsyncLogger asyncLogger;

    public FIXOrderServer(LFQueue<OrderRequest> clientRequests,
                          LFQueue<OrderMessage> clientResponses,
                          LeadershipManager leadershipManager,
                          AppState appState,
                          AsyncLogger asyncLogger) {
        this.asyncLogger = asyncLogger;
        this.clientRequests = clientRequests;
        this.clientResponses = clientResponses;
        this.leadershipManager = leadershipManager;
        this.appState = appState;
        this.clientResponses.subscribe(this::processResponse);
    }

    @Override
    public synchronized void start() {
        leadershipManager.onLeadershipAcquired(() -> {
            boolean wasRunning = stopReplicationConsumer();
            if (!wasRunning) {
                log.info("start. stopReplicationConsumer. wasRunning == false");
                appState.setRecovering();
                ReplayReplicationLogConsumer replayConsumer = new ReplayReplicationLogConsumer(clientRequests);
                replayConsumer.run();
                this.seqNum = replayConsumer.getLastSeqNum();
            }
            appState.setRecovered();
            startRequestSequencer();
            startFIXAcceptor();
        });

        leadershipManager.onLeadershipLost(() -> {
            stopFIXAcceptor();
            stopRequestSequencer();
            startReplicationConsumer();
        });
    }

    private void startFIXAcceptor() {
        try {
            final var thisHost = System.getenv().get("THISHOST");
            SessionSettings settings;
            if (thisHost.endsWith("1")) {
                settings = new SessionSettings("fix-server1.cfg");
            } else if (thisHost.endsWith("2")) {
                settings = new SessionSettings("fix-server2.cfg");
            } else {
                throw new IllegalStateException("Unknown host: " + thisHost);
            }
            log.info(" {} | Starting FIX acceptor with settings: {}", thisHost, settings);
            MessageStoreFactory storeFactory = new FileStoreFactory(settings);
//            LogFactory logFactory = new ScreenLogFactory(true, true, true);
            LogFactory logFactory = new PipeDelimitedScreenLogFactory(asyncLogger);
            MessageFactory messageFactory = new ReusableMessageFactory(new Message());

            Application application = new FIXApplicationAdapter();

            acceptor = new ThreadedSocketAcceptor(application, storeFactory, settings, logFactory, messageFactory);
            acceptor.start();
            log.info("FIX acceptor started.");
        } catch (Exception e) {
            log.error("Failed to start FIX acceptor", e);
        }
    }

    private void stopFIXAcceptor() {
        if (acceptor != null) {
            acceptor.stop();
            acceptor = null;
            log.info("FIX acceptor stopped.");
        }
    }

    private synchronized void startReplicationConsumer() {
        replicationConsumer = new ReplicationConsumer(clientRequests, appState);
        replicationConsumer.run();
    }

    private synchronized boolean stopReplicationConsumer() {
        if (replicationConsumer == null) {
            log.info("stopReplicationConsumer. replicationConsumer == null");
            return false;
        }
        replicationConsumer.shutdown();
        this.seqNum = replicationConsumer.getLastSeqNum();
        replicationConsumer = null;
        return true;
    }

    private synchronized void startRequestSequencer() {
        requestSequencer = new RequestSequencer(clientRequests, seqNum, appState);
        requestSequencerThread = new Thread(requestSequencer, "RequestSequencerThread");
        requestSequencerThread.start();
    }

    private synchronized void stopRequestSequencer() {
        if (requestSequencer != null) {
            requestSequencer.shutdown();
        }
        if (requestSequencerThread != null && requestSequencerThread.isAlive()) {
            requestSequencerThread.interrupt();
            try {
                requestSequencerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        requestSequencerThread = null;
        requestSequencer = null;
    }

    private void processResponse(OrderMessage orderMessage) {
        SessionHolder sessionHolder = null;
        try {
            if (appState.isNotRecoveredLeader()) {
                if (log.isDebugEnabled()) {
                    log.debug("processResponse. isNotRecoveredLeader. Not publishing {}", orderMessage);
                }
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Start Sending {}", orderMessage);
            }
            if (orderMessage == null) {
                log.info("processResponse. orderMessage == null");
            }

            long seq = respSeqNum.getAndIncrement();
            orderMessage.setSeqNum(seq);

            Long clientId = orderMessage.getClientId();
            sessionHolder = sessionsByClientId.get(clientId);
            SessionID sessionId = sessionHolder.sessionId;
            if (sessionId == null) {
                log.warn("No session for clientId={}", clientId);
                return;
            }

            Message msg = sessionHolder.reusableOutMessage;
            OrderMessageSerDe.toFIXMessage(orderMessage, msg);

            Session.sendToTarget(msg, sessionId);
            if (log.isDebugEnabled()) {
                log.debug("Sending FIX response to clientId={}: {} {}", clientId,
                        msg.toString().replace('\u0001', '|'), orderMessage);
            }
        } catch (Exception e) {
            log.error("Failed to send FIX OrderMessage, " + orderMessage + " sessionHolder", e);
        }
    }

    public synchronized void shutdown() {
        stopFIXAcceptor();
        stopRequestSequencer();
    }

    private class FIXApplicationAdapter extends ApplicationAdapter {

        @Override
        public void onLogon(SessionID sessionId) {
            log.info("FIX session logged in: " + sessionId);
        }

        @Override
        public void onLogout(SessionID sessionId) {
            log.info("FIX session logged out: " + sessionId);
        }

        @Override
        public void fromApp(Message message, SessionID sessionId) {
            if (log.isDebugEnabled()) {
                log.debug("Received App Message: " + message.toString().replace('\u0001', '|'));
            }
            try {

                OrderRequest request = OrderRequestSerDe.getOrderRequest(message);

                if (log.isDebugEnabled()) {
                    log.debug("Parsed client request: {}", request);
                }

                long clientId = request.getClientId();
                sessionsByClientId.computeIfAbsent(clientId, clId -> {
                    log.info("Client ID {} registered for session {}", clientId, sessionId);
                    return new SessionHolder(sessionId, clientId, new Message());
                });


                if (requestSequencer != null) {
                    requestSequencer.process(request);
                } else {
                    log.warn("RequestSequencer not active, ignoring request from clientId={}", request);
                }

            } catch (Exception e) {
                log.error("fromApp. Failed " + message + " + " + sessionId, e);
            }
        }
    }

}
