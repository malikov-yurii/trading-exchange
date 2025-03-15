package trading.participant.strategy;

import trading.common.Constants;

public class TradeEngineConfigMap {

    private final TradeEngineConfig[] tradeEngineConfigs;

    public TradeEngineConfigMap() {
        this.tradeEngineConfigs = new TradeEngineConfig[Constants.ME_MAX_TICKERS];
        for(int i = 0; i < tradeEngineConfigs.length; i++) {
            tradeEngineConfigs[i] = new TradeEngineConfig(Constants.CLIP_ORDER_QTY, 1, RiskConfig.DEFAULT_RISK_CONFIG);
        }
    }

    public TradeEngineConfig get(long tickerId) {
        return tradeEngineConfigs[tickerToIndex(tickerId)];
    }

    private static int tickerToIndex(long tickerId) {
        return (int) (tickerId % Constants.ME_MAX_TICKERS);
    }

}
