package trading.participant;

import com.lmax.disruptor.dsl.ProducerType;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;
import trading.common.DisruptorLFQueue;
import trading.common.LFQueue;
import trading.participant.marketdata.MarketDataConsumer;
import trading.participant.ordergateway.OrderGatewayClient;
import trading.participant.strategy.AlgoType;
import trading.participant.strategy.TradeEngine;
import trading.participant.strategy.TradeEngineUpdate;

import java.util.concurrent.atomic.AtomicLong;

public class ParticipantApplication {
    private static final Logger log = LoggerFactory.getLogger(ParticipantApplication.class);
    private static final String ORDER_SERVER_URI = "ws://localhost:8080/ws";
    private final AtomicLong orderSeqNum = new AtomicLong(1);

    public static void main(String[] args) throws Exception {

        LFQueue<TradeEngineUpdate> tradeEngineUpdates = new DisruptorLFQueue<>(1024, "tradeEngineUpdates", ProducerType.MULTI);
        MarketDataConsumer marketDataConsumer = new MarketDataConsumer(tradeEngineUpdates);
        Thread marketDataConsumerThread = new Thread(marketDataConsumer);
        marketDataConsumerThread.start();

//        MarketDataSnapshotConsumer marketDataSnapshotConsumer = new MarketDataSnapshotConsumer();
//        Thread marketDataSnapshotConsumerThread = new Thread(marketDataSnapshotConsumer);
//        marketDataSnapshotConsumerThread.start();

        Thread.sleep(200); // TODO Improve waiting for MarketDataConsumer to start

        LFQueue<OrderRequest> orderRequests = new DisruptorLFQueue<>(1024, "orderRequests", ProducerType.MULTI);

        OrderGatewayClient orderGatewayClient = new OrderGatewayClient(ORDER_SERVER_URI, orderRequests, tradeEngineUpdates);
        orderGatewayClient.start();


        AlgoType algoType = AlgoType.MARKET_MAKER;
        int clientId = 1;
        TradeEngine tradeEngine = new TradeEngine(algoType, orderRequests, tradeEngineUpdates, clientId);

        tradeEngineUpdates.init();
        orderRequests.init();

        sendTestOrders(orderGatewayClient);

//        Thread.sleep(5_000);
        log.info("ParticipantApplication is running. tradeEngine lastUpdateTime {}", tradeEngine.getLastUpdateTime());
        new ShutdownSignalBarrier().await();
        orderGatewayClient.shutdown();
        marketDataConsumer.shutdown();
//        marketDataSnapshotConsumer.shutdown();
//        marketDataSnapshotConsumerThread.interrupt();
        orderRequests.shutdown();
        tradeEngineUpdates.shutdown();
        log.info("ParticipantApplication terminated. tradeEngine lastUpdateTime {}", tradeEngine.getLastUpdateTime());
        System.exit(0);
    }

    private static void sendTestOrders(OrderGatewayClient orderGatewayClient) {
        int orderId = 0;
        int numOrders = 1; // Send N buy orders, then N sell orders
        for (int i = 0; i < numOrders; i++) {
            OrderRequest orderRequest = new OrderRequest();
            orderRequest.setType(OrderRequestType.NEW);
            orderRequest.setClientId(0);
            orderRequest.setOrderId(orderId++);
            orderRequest.setSide(Side.SELL);
            orderRequest.setPrice(100 + i);
            orderRequest.setQty(100);
            orderGatewayClient.sendOrderRequest(orderRequest);

            orderRequest.setSide(Side.BUY);
            orderRequest.setQty(10);
            orderGatewayClient.sendOrderRequest(orderRequest);
        }
    }

}