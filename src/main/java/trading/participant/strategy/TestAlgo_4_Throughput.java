package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;

public class TestAlgo_4_Throughput extends TestAlgo {
    private static final Logger log = LoggerFactory.getLogger(TestAlgo_4_Throughput.class);

    @Override
    public void run() {
        try {
            log.info("TestAlgo_4_Throughput. run. Thread: {}", Thread.currentThread().getName());
            final int getTotalOrders = getOrderNum();
            while (isRunning()) {
                if (!(getNextOrderId().get() < getTotalOrders)) {
                    break;
                }
                OrderRequest sellNewOrderRequest = new OrderRequest();
                sendNewOrderRequest(sellNewOrderRequest, 0, 0, Side.SELL, 100, 10);

                OrderRequest buyNewOrderRequest = new OrderRequest();
                sendNewOrderRequest(buyNewOrderRequest, 1, 0, Side.BUY, 30, 10);

                OrderRequest cancelSellNewOrderRequest = new OrderRequest(sellNewOrderRequest);
                cancelSellNewOrderRequest.setType(OrderRequestType.CANCEL);
                sendOrderRequest(cancelSellNewOrderRequest);
            }

        } catch (Exception e) {
            log.error("Error in TestAlgo", e);
        }
    }

    public TestAlgo_4_Throughput(TradeEngine tradeEngine) {
        super(tradeEngine);
    }

    @Override
    public void onOrderBookUpdate(long tickerId, long price, Side side, MarketOrderBook marketOrderBook) {
    }

    @Override
    public void onTradeUpdate(MarketUpdate marketUpdate, MarketOrderBook marketOrderBook) {
    }

}
