package trading.participant;

import aeron.AeronClient;
import com.lmax.disruptor.dsl.ProducerType;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.common.DisruptorLFQueue;
import trading.common.DisruptorLogger;
import trading.common.LFQueue;
import trading.participant.marketdata.MarketDataConsumer;
import trading.participant.ordergateway.FIXOrderGatewayClient;
import trading.participant.ordergateway.OrderGatewayClient;
import trading.participant.strategy.AlgoType;
import trading.participant.strategy.TradeEngine;
import trading.participant.strategy.TradeEngineUpdate;

public class ParticipantApplication {
    private static final Logger log = LoggerFactory.getLogger(ParticipantApplication.class);

    public static void main(String[] args) throws Exception {

        DisruptorLogger asyncLogger = new DisruptorLogger( 10);

        LFQueue<TradeEngineUpdate> tradeEngineUpdates = new DisruptorLFQueue<>(null,
                "tradeEngineUpdates", ProducerType.MULTI, TradeEngineUpdate::new, TradeEngineUpdate::copy);
        MarketDataConsumer marketDataConsumer = new MarketDataConsumer(tradeEngineUpdates);
        Thread marketDataConsumerThread = new Thread(marketDataConsumer);
        marketDataConsumerThread.start();

//        MarketDataSnapshotConsumer marketDataSnapshotConsumer = new MarketDataSnapshotConsumer();
//        Thread marketDataSnapshotConsumerThread = new Thread(marketDataSnapshotConsumer);
//        marketDataSnapshotConsumerThread.start();

        Thread.sleep(200); // TODO Improve waiting for MarketDataConsumer to start

        LFQueue<OrderRequest> orderRequests = new DisruptorLFQueue<>(null, "orderRequests",
                ProducerType.MULTI, OrderRequest::new, OrderRequest::copy);

        OrderGatewayClient orderGatewayClient = new FIXOrderGatewayClient(orderRequests, tradeEngineUpdates, asyncLogger);
        orderGatewayClient.start();


        AlgoType algoType = AlgoType.valueOf(env("ALGO_TYPE", "MARKET_MAKER"));
        int clientId = Integer.parseInt(env("CLIENT_ID", "1"));

        TradeEngine tradeEngine = new TradeEngine(algoType, orderRequests, tradeEngineUpdates, clientId, orderGatewayClient);

        asyncLogger.init();
        tradeEngineUpdates.init();
        orderRequests.init();
        tradeEngine.init();

        log.info("ParticipantApplication is running. tradeEngine lastUpdateTime {}", tradeEngine.getLastUpdateTime());
        new ShutdownSignalBarrier().await();
        orderGatewayClient.shutdown();
        marketDataConsumer.shutdown();
//        marketDataSnapshotConsumer.shutdown();
//        marketDataSnapshotConsumerThread.interrupt();
        orderRequests.shutdown();
        tradeEngineUpdates.shutdown();
        AeronClient.AERON_INSTANCE_REMOTE.close();
        log.info("ParticipantApplication terminated. tradeEngine lastUpdateTime {}", tradeEngine.getLastUpdateTime());
        asyncLogger.shutdown();
        System.exit(0);
    }

    private static String env(String envVar, String defaultValue) {
        return ObjectUtils.defaultIfNull(System.getenv(envVar), defaultValue);
    }

}