package trading.participant.strategy;

import trading.api.Side;

public class RiskManager {

    private final RiskInfo[] tickerRiskInfos;

    public RiskManager(int numTickers, PositionManager positionManager, TradeEngineConfigMap tradeEngineConfigMap) {
        this.tickerRiskInfos = new RiskInfo[numTickers];
        for (int tickerId = 0; tickerId < numTickers; tickerId++) {
            TradeEngineConfig tradeEngineConfig = tradeEngineConfigMap.get(tickerId);
            tickerRiskInfos[tickerId] = new RiskInfo(tradeEngineConfig.getRiskConfig(), positionManager.getPosition(tickerId));
        }
    }

    public RiskInfo.RiskCheckResult checkPreTradeRisk(long tickerId, Side side, long qty) {
        RiskInfo tickerRiskInfo = tickerRiskInfos[(int) tickerId];
        return tickerRiskInfo.checkPreTradeRisk(side, qty);
    }

}
