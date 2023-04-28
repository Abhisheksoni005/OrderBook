package org.trade;

import junit.framework.TestCase;
import org.junit.Test;

public class OrderBookTest extends TestCase {

    @Test
    public void testInitializeSnapshot() {
        OrderBook orderBook = new OrderBook();
        String snapshotMessage = "{\"lastUpdateId\":123,\"bids\":[[0.1,1.0],[0.2,2.0]],\"asks\":[[0.3,3.0],[0.4,4.0]]}";
        orderBook.initializeSnapshot(snapshotMessage);
        assertEquals(123, orderBook.getLastUpdateId());
        assertEquals(2, orderBook.getBidsMap().size());
        assertEquals(1.0, orderBook.getBidsMap().get(0.1));
        assertEquals(2.0, orderBook.getBidsMap().get(0.2));
        assertEquals(2, orderBook.getAsksMap().size());
        assertEquals(3.0, orderBook.getAsksMap().get(0.3));
        assertEquals(4.0, orderBook.getAsksMap().get(0.4));
    }

    @Test
    public void testBufferEvents() {
        OrderBook orderBook = new OrderBook();
        String eventMessage = "{\"u\":123,\"U\":124,\"b\":[[0.1,2.0]],\"a\":[[0.2,3.0]]}";
        orderBook.bufferEvents(eventMessage);
        assertEquals(1, orderBook.getEventQueue().size());
    }

    @Test
    public void testProcessEvents() throws OrderBookException {
        OrderBook orderBook = new OrderBook();
        orderBook.setLastUpdateId(123L);
        orderBook.getBidsMap().put(0.1, 1.0);
        orderBook.getAsksMap().put(0.2, 2.0);
        String eventMessage = "{\"u\":125,\"U\":124,\"b\":[[0.1,3.0]],\"a\":[[0.2,4.0]]}";
        orderBook.bufferEvents(eventMessage);
        orderBook.processEvents();
        assertEquals(125, orderBook.getLastUpdateId());
        assertEquals(3.0, orderBook.getBidsMap().get(0.1));
        assertEquals(4.0, orderBook.getAsksMap().get(0.2));
    }
}
