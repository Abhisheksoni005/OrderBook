package org.trade;

import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;

@Slf4j
public class OrderBookListener extends WebSocketClient {

    private final OrderBook orderBook;

    public OrderBookListener(OrderBook orderBook) {
        super(URI.create(orderBook.binanceEndpoint));
        this.orderBook = orderBook;
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        log.info("Websocket connection to server opened");
    }

    @Override
    public void onMessage(String message) {
        this.orderBook.bufferEvents(message);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        log.info("Websocket connection to server closed");
    }

    @Override
    public void onError(Exception ex) {
        log.info("Error occurred during an open connection: ", ex);
    }
}
