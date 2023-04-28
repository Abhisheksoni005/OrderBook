package org.trade;

public class OrderBookException extends Exception {

    public OrderBookException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public static class OrderBookExecutionException extends RuntimeException {
        public OrderBookExecutionException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }

}
