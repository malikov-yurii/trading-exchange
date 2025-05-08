package trading.participant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestApplication {
    private static final Logger log = LoggerFactory.getLogger(TestApplication.class);

    public static void main(String[] args) throws Exception {
//        log.info("TestApplication is starting.");
//        LFQueue<TradeEngineUpdate> tradeEngineUpdates = new DisruptorLFQueue<>(18, "tradeEngineUpdates", ProducerType.MULTI);
//
//        tradeEngineUpdates.subscribe(tradeEngineUpdate -> {
//            log.info("Received TradeEngineUpdate: {}", tradeEngineUpdate);
//        });
//        tradeEngineUpdates.init();
//
//        new Thread(() -> {
//            for (int i = 0; i < 4; i++) {
//                MarketUpdate marketUpdate = new MarketUpdate();
//                marketUpdate.setSeqNum(i);
//                tradeEngineUpdates.offer(new TradeEngineUpdate(marketUpdate, null));
//            }
//        }).start();
//
//        new Thread(() -> {
//            for (int i = 10; i < 14; i++) {
//                OrderMessage orderMessage = new OrderMessage();
//                orderMessage.setSeqNum(i);
//                tradeEngineUpdates.offer(new TradeEngineUpdate(null, orderMessage));
//            }
//        }).start();
//
//        log.info("TestApplication is running.");
//        new ShutdownSignalBarrier().await();
//
//        System.exit(0);
    }

}