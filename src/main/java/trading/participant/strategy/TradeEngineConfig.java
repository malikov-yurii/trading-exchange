package trading.participant.strategy;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TradeEngineConfig {

    private final long clip;
    private final long threshold;
    private final RiskConfig riskConfig;

    @Override
    public String toString() {
        return "TradeEngineConfig{" +
                "clip=" + clip +
                ", riskConfig=" + riskConfig +
                ", threshold=" + threshold +
                '}';
    }
}
