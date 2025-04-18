package trading.participant.ordergateway;

import trading.api.OrderRequest;

public interface OrderGatewayClient {

    void sendOrderRequest(OrderRequest orderRequest);

    void shutdown();

    void start();
}
