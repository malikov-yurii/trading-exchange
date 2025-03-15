package trading.participant.strategy;

import trading.api.OrderMessage;
import trading.common.Constants;

public class PositionManager {

    private final Position[] positions;

    public PositionManager() {
        positions = new Position[Constants.ME_MAX_TICKERS];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = new Position();
        }

    }

    public void updateBBO(long tickerId, MarketOrderBook.BBO bbo) {
        Position position = positions[(int) tickerId];
        position.updateBBO(bbo);
    }

    public void addFill(OrderMessage orderMessage) {
        Position position = positions[(int) orderMessage.getTickerId()];
        position.addFill(orderMessage);
    }

    public Position getPosition(int i) {
        return positions[i];
    }

}
