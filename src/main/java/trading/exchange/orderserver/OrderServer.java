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
import trading.api.OrderMessage;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.common.LFQueue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BablOrderServer that starts on a separate thread so that start() returns immediately.
 */
public class OrderServer implements Application {
    private static final Logger log = LoggerFactory.getLogger(OrderServer.class);

    private final LFQueue<OrderRequest> requestQueue;
    private final LFQueue<OrderMessage> responseQueue;

    private final AtomicLong reqSeqNum = new AtomicLong(1);
    private final AtomicLong respSeqNum = new AtomicLong(1);

    private final Map<Long, Session> sessionsByClientId = new ConcurrentHashMap<>();
    private final Map<Long, Long> clientIdBySessionId = new ConcurrentHashMap<>();

    private Thread serverThread;
    private SessionContainers containers;
    private final String bindAddress;
    private final int listenPort;

    public OrderServer(LFQueue<OrderRequest> requestQueue, LFQueue<OrderMessage> responseQueue,
                       String bindAddress, int listenPort) {
        this.requestQueue = requestQueue;
        this.responseQueue = responseQueue;
        this.bindAddress = bindAddress;
        this.listenPort = listenPort;
        // Subscribe to inbound responses so we can forward them
        responseQueue.subscribe(this::processResponse);
    }

    public void start() {
        final BablConfig config = new BablConfig();
        config.sessionContainerConfig().bindAddress(bindAddress);
        config.sessionContainerConfig().listenPort(listenPort);
        config.applicationConfig().application(this);
        config.performanceConfig().performanceMode(PerformanceMode.DEVELOPMENT);

        serverThread = new Thread(() -> {
            try {
                containers = BablServer.launch(config);
                containers.start();
                log.info("BablOrderServer started.");
                new ShutdownSignalBarrier().await();
            } catch (Exception e) {
                log.error("Error in BablOrderServer thread", e);
            } finally {
                if (containers != null) {
                    containers.close();
                }
                log.info("BablOrderServer thread has exited.");
            }
        }, "BablOrderServerThread");

        serverThread.start();
    }

    public void shutdown() {
        log.info("Shutting down BablOrderServer...");
        try {
            if (serverThread != null && serverThread.isAlive()) {
                serverThread.interrupt();
                serverThread.join(2000);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down BablOrderServer", e);
            Thread.currentThread().interrupt();
        }
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
//            log.info("Received message from session {} ({} bytes): {}",
//                    session.id(), length, hexDump(msg, offset, length));

            OrderRequest orderRequest = deserializeClientRequest(msg, offset, length);
            long seq = reqSeqNum.getAndIncrement();
            orderRequest.setSeqNum(seq);

            if (!sessionsByClientId.containsKey(orderRequest.getClientId())) {
                log.info("First request from clientId={}", orderRequest.getClientId());
                clientIdBySessionId.put(session.id(), orderRequest.getClientId());
                sessionsByClientId.put(orderRequest.getClientId(), session);
            }

            log.info("Received ClientRequest: {}", orderRequest);

            requestQueue.offer(orderRequest);
        } catch (Exception e) {
            log.error("Error processing message from session {}: {}", session.id(), e.getMessage(), e);
        }
        return SendResult.OK;
    }

    private void processResponse(OrderMessage orderMessage) {
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

        // Key fix: allocate enough initial capacity for large messages
        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
        int offset = 0;

        buffer.putLong(offset, orderMessage.getSeqNum());
        offset += Long.BYTES;

        buffer.putByte(offset, (byte) orderMessage.getType().ordinal());
        offset += Byte.BYTES;

        buffer.putByte(offset, (byte) orderMessage.getSide().ordinal());
        offset += Byte.BYTES;

        buffer.putLong(offset, orderMessage.getClientId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getTickerId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getClientOrderId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getMarketOrderId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getPrice());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getExecQty());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getLeavesQty());
        offset += Long.BYTES;

        int length = offset;
        ContentType binary = ContentType.BINARY;

        // Attempt to send the data
        int sendResult = session.send(binary, buffer, 0, length);

        log.info("Sent response {}. {}", orderMessage, sendResult == 0 ? "OK" : "FAILED");
    }

    /**
     * Convert the buffer to a hex string for debugging
     */
    private String hexDump(DirectBuffer buffer, int offset, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            byte b = buffer.getByte(offset + i);
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }

    private OrderRequest deserializeClientRequest(final DirectBuffer data, int offset, int length) {
        OrderRequest req = new OrderRequest();

        // Read request type first (1 byte)
        byte requestType = data.getByte(offset);
        offset += Byte.BYTES;
        OrderRequestType type = OrderRequestType.fromValue(requestType);
        req.setType(type);
        log.info("deserializeClientRequest. requestType={}, type={}", requestType, type);

        // Read sequence number (8 bytes)
        long seq = data.getLong(offset);
        offset += Long.BYTES;
        req.setSeqNum(seq);

        // Read side (1 byte)
        byte sideOrd = data.getByte(offset);
        offset += Byte.BYTES;
        req.setSide(Side.values()[sideOrd]);

        // Read clientId (8 bytes)
        req.setClientId(data.getLong(offset));
        offset += Long.BYTES;

        // Read tickerId (8 bytes)
        req.setTickerId(data.getLong(offset));
        offset += Long.BYTES;

        // Read orderId (8 bytes)
        req.setOrderId(data.getLong(offset));
        offset += Long.BYTES;

        // Read price (8 bytes)
        req.setPrice(data.getLong(offset));
        offset += Long.BYTES;

        // Read qty (8 bytes)
        req.setQty(data.getLong(offset));
        offset += Long.BYTES;

        return req;
    }
}