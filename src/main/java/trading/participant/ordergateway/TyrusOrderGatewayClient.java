package trading.participant.ordergateway;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderMessage;
import trading.api.OrderMessageSerDe;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.participant.strategy.TradeEngineUpdate;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@ClientEndpoint
public class TyrusOrderGatewayClient implements OrderGatewayClient {
    private static final Logger log = LoggerFactory.getLogger(TyrusOrderGatewayClient.class);

    private final LFQueue<TradeEngineUpdate> tradeEngineUpdates;
    private final AtomicLong orderSeqNum = new AtomicLong(1);
    private Session session;
    private final TradeEngineUpdate t = new TradeEngineUpdate();

    public TyrusOrderGatewayClient(LFQueue<OrderRequest> orderRequests,
                                   LFQueue<TradeEngineUpdate> tradeEngineUpdates) {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            List<String> orderServerUris = Utils.getOrderServerUris();
            log.info("Connecting to Order Gateway WebSocket: {}", orderServerUris);
            container.connectToServer(this, new URI(orderServerUris.get(0)));
        } catch (Exception e) {
            log.error("Failed to connect to Order Gateway WebSocket: {}", e.getMessage());
        }
        this.tradeEngineUpdates = tradeEngineUpdates;
        // Subscribe to requests
        orderRequests.subscribe(this::sendOrderRequest);

    }

    @OnOpen
    public void onOpen(Session session) {
        this.session = session;
        log.info("Connected to Order Gateway WebSocket.");
    }

    @OnMessage
    public void onMessage(ByteBuffer message) {
        // Force little-endian on the input buffer
        log.info("Received message: {}", message);
        message.order(ByteOrder.LITTLE_ENDIAN);
        // Convert ByteBuffer to Netty ByteBuf
//        byte[] arr = new byte[message.remaining()];
//        message.get(arr);
        ByteBuf nettyBuf = Unpooled.wrappedBuffer(message);
        OrderMessage orderMessage = OrderMessageSerDe.parseOrderMessage(nettyBuf);
        log.info("Received: " + orderMessage);
        t.set(orderMessage);
        tradeEngineUpdates.offer(t);
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        log.info("WebSocket closed: " + closeReason);
    }

    @OnError
    public void onError(Session session, Throwable thr) {
        log.error("WebSocket error: " + thr.getMessage());
    }

    @Override
    public void sendOrderRequest(OrderRequest request) {
        try {
            long seq = orderSeqNum.getAndIncrement();
            request.setSeqNum(seq);
            ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
            int len = OrderRequestSerDe.serialize(request, buffer, 0);
            byte[] data = new byte[len];
            buffer.getBytes(0, data);
            session.getAsyncRemote().sendBinary(ByteBuffer.wrap(data));
            log.info("Sent: " + request);
        } catch (Exception e) {
            log.error("Failed to send order: " + e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        try {
            if (session != null) {
                session.close(new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "Client shutdown"));
            }
        } catch (Exception e) {
            log.error("Error closing WebSocket: {}", e.getMessage());
        }
    }

    @Override
    public void start() {

    }
}