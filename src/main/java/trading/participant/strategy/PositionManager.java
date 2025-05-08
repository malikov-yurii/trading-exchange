package trading.participant.strategy;

import trading.api.OrderResponse;
import trading.common.Constants;

public class PositionManager {

    private final Position[] positions;

    public PositionManager() {
        positions = new Position[Constants.ME_MAX_TICKERS];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = new Position();
        }

    }

    public void onBBOUpdate(long tickerId, MarketOrderBook.BBO bbo) {
        Position position = positions[(int) tickerId];
        position.updatePnl(bbo);
    }

    public void addFill(OrderResponse orderResponse) {
        Position position = positions[(int) orderResponse.getTickerId()];
        position.addFill(orderResponse);
    }

    public Position getPosition(int i) {
        return positions[i];
    }

}
