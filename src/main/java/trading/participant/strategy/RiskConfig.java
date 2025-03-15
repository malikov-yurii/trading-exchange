package trading.participant.strategy;

import lombok.Getter;
import trading.common.Constants;

@Getter
public class RiskConfig {

    public static final RiskConfig DEFAULT_RISK_CONFIG = new RiskConfig(
            Constants.DEFAULT_MAX_ORDER_QTY_LIMIT,
            Integer.MAX_VALUE,
            Integer.MIN_VALUE);

    private final double maxOrderQty;
    private final double maxPosition;
    private final double maxLoss;

    public RiskConfig(double maxOrderQty, double maxPosition, double maxLoss) {
        this.maxOrderQty = maxOrderQty;
        this.maxPosition = maxPosition;
        this.maxLoss = maxLoss;
    }

}
