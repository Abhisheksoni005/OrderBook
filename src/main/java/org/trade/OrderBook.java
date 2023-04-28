package org.trade;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.java_websocket.client.WebSocketClient;
import org.trade.OrderBookException.OrderBookExecutionException;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Getter
public class OrderBook {
    public static final String SYMBOL = "BNBBTC";
    private static final Long LIMIT = 20L;
    private static final int SKIP_LIMIT = 5;

    @Setter
    private long lastUpdateId = -1L;
    private boolean processing = false;
    private int reprocess = SKIP_LIMIT;
    private CountDownLatch latch = new CountDownLatch(1);

    private final ObjectMapper objectMapper;
    private final TreeMap<Double, Double> bidsMap;
    private final TreeMap<Double, Double> asksMap;
    private final BlockingQueue<JsonNode> eventQueue;
    public final String binanceEndpoint;
    public final String snapshotUrl;

    public OrderBook() {
        objectMapper = new ObjectMapper();
        bidsMap = new TreeMap<Double, Double>(Collections.<Double>reverseOrder());
        asksMap = new TreeMap<Double, Double>();
        eventQueue = new LinkedBlockingQueue<JsonNode>();
        binanceEndpoint = "wss://stream.binance.com:9443/ws/" + SYMBOL.toLowerCase() + "@depth@100ms";
        snapshotUrl = "https://api.binance.com/api/v3/depth?symbol=" + SYMBOL + "&limit=" + LIMIT;
    }

    private void open() throws OrderBookException {

        try {
            WebSocketClient webSocketClient = new OrderBookListener(this);
            webSocketClient.connect();

            //buffer time so we get some events before the snapshot
            Thread.sleep(5000);
            this.getSnapshot();
            latch.countDown();

        } catch (Exception e) {
            log.info("Error while creating connection to the server: ", e);
            throw new OrderBookException("Exception occurred while establishing websocket connection", e);
        }
    }

    private void start() throws OrderBookException {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new OrderBookExecutionException("Exception occurred during setup", e);
        }
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this::logOrderBook, 0, 5, TimeUnit.SECONDS);
        while (true) {
            this.processEvents();
        }
    }

    protected void bufferEvents(String message) {
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(message);
            this.eventQueue.put(jsonNode);
        } catch (IOException | InterruptedException e) {
            throw new OrderBookExecutionException("Exception occurred while storing events in a queue", e);
        }
    }

    private void getSnapshot() throws OrderBookException {

        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            HttpGet request = new HttpGet(this.snapshotUrl);
            CloseableHttpResponse httpResponse = httpClient.execute(request);
            if (httpResponse.getEntity() != null) {
                String result = EntityUtils.toString(httpResponse.getEntity());
                this.initializeSnapshot(result);
            }
        } catch (Exception e) {
            log.info("Http request failed");
            throw new OrderBookException("Exception occurred while fetching snapshot from binance api", e);
        }
    }

    protected synchronized void initializeSnapshot(String message) {
        this.bidsMap.clear();
        this.asksMap.clear();

        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(message);
        } catch (IOException e) {
            throw new OrderBookExecutionException("Exception occurred while serialising the response json", e);
        }
        this.lastUpdateId = jsonNode.get("lastUpdateId").asLong();

        JsonNode bids = jsonNode.get("bids");
        for (JsonNode bid : bids) {
            double price = bid.get(0).asDouble();
            double size = bid.get(1).asDouble();
            this.bidsMap.put(price, size);
        }

        JsonNode asks = jsonNode.get("asks");
        for (JsonNode ask : asks) {
            double price = ask.get(0).asDouble();
            double size = ask.get(1).asDouble();
            this.asksMap.put(price, size);
        }
    }

    protected void processEvents() throws OrderBookException {
        JsonNode jsonNode;
        try {
            jsonNode = this.eventQueue.take();
        } catch (InterruptedException e) {
            throw new OrderBookExecutionException("Exception occurred while serialising the response json", e);
        }

        long updateId = jsonNode.get("u").asLong();
        long firstUpdateId = jsonNode.get("U").asLong();

        if (updateId > this.lastUpdateId) {
            synchronized (this) {
                if (this.lastUpdateId < 0) {
                    return;
                }
                //first processed event
                if (!processing && firstUpdateId <= this.lastUpdateId) {

                    this.updateTree(jsonNode.get("b"), this.bidsMap);
                    this.updateTree(jsonNode.get("a"), this.asksMap);
                    this.lastUpdateId = updateId;
                    processing = true;

                } else if (firstUpdateId == this.lastUpdateId + 1) {

                    this.updateTree(jsonNode.get("b"), this.bidsMap);
                    this.updateTree(jsonNode.get("a"), this.asksMap);
                    this.lastUpdateId = updateId;

                } else {
                    //missing events
                    //keeping a buffer of 5 events to be skipped in order to look for the next valid event
                    this.reprocess -= 1;
                    if (this.reprocess == 0) {
                        this.reprocess = 5;
                        this.getSnapshot();
                    }
                }
            }
        }
    }

    private void updateTree(JsonNode node, TreeMap<Double, Double> map) {
        for (JsonNode n : node) {
            double price = n.get(0).asDouble();
            double size = n.get(1).asDouble();
            if (size == 0) {
                map.remove(price);
            } else {
                map.put(price, size);
            }
        }
    }

    private void logOrderBook() {
        long numBids = Math.min(this.bidsMap.size(), LIMIT);
        long numAsks = Math.min(this.asksMap.size(), LIMIT);
        List<Map.Entry<Double, Double>> bidList = new ArrayList<>(this.bidsMap.entrySet());
        List<Map.Entry<Double, Double>> askList = new ArrayList<>(this.asksMap.entrySet());

        System.out.printf("%-15s %15s  %-15s %15s%n", "BID_SIZE", "BID_PRICE", "ASK_PRICE", "ASK_SIZE");
        for (int i = 0; i < Math.max(numAsks, numBids); i++) {
            if (i < numBids) {
                Map.Entry<Double, Double> bid = bidList.get(i);
                String bidSizeStr = formatNumber(bid.getValue());
                String bidPriceStr = formatNumber(bid.getKey());
                System.out.printf("%-15s %15s  ", bidSizeStr, bidPriceStr);
            } else {
                System.out.printf("%-15s %15s  ", "", "");
            }

            if (i < numAsks) {
                Map.Entry<Double, Double> ask = askList.get(i);
                String askPriceStr = formatNumber(ask.getKey());
                String askSizeStr = formatNumber(ask.getValue());
                System.out.printf("%-15s %15s", askPriceStr, askSizeStr);
            } else {
                System.out.printf("%-15s %15s", "", "");
            }
            System.out.println();
        }
        System.out.println();
    }

    private String formatNumber(Double number) {
        DecimalFormat format = new DecimalFormat("0.00000000");
        return format.format(number);
    }

    //Run this to start printing the Order Book
    public static void main(String args[]) throws OrderBookException {
        OrderBook orderBook = new OrderBook();
        orderBook.open();
        orderBook.start();
    }
}


