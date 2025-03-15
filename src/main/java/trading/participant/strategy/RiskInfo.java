package trading.participant.strategy;

import trading.api.Side;

public class RiskInfo {

    private final RiskConfig riskConfig;
    private final Position position;

    public RiskInfo(RiskConfig riskConfig, Position position) {
        this.riskConfig = riskConfig;
        this.position = position;
    }

    public RiskCheckResult checkPreTradeRisk(Side side, long qty) {
        if (qty > riskConfig.getMaxOrderQty()) {
            return RiskCheckResult.RISK_EXCEED_MAX_ORDER_QTY;
        }
        if (position.getPosition() + side.getSign() * qty > riskConfig.getMaxPosition()) {
            return RiskCheckResult.RISK_EXCEED_MAX_POSITION;
        }
        if (position.getTotalPnl() < riskConfig.getMaxLoss()) {
            return RiskCheckResult.RISK_EXCEED_MAX_ALLOWED_LOSS;
        }
        return RiskCheckResult.RISK_ALLOWED;
    }

    public enum RiskCheckResult {
        RISK_ALLOWED,
        RISK_EXCEED_MAX_ORDER_QTY,
        RISK_EXCEED_MAX_POSITION,
        RISK_EXCEED_MAX_ALLOWED_LOSS
    }

}
