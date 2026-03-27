package com.stock;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

public class StockProducer {
    private static final String TOPIC = "stock-ticks";
    private static final Gson gson = new Gson();
    private static final ConcurrentHashMap<String, Double> stockPrices = new ConcurrentHashMap<>();
    private static final String[] SYMBOLS = {
        "VIC", "VNM", "FPT", "HPG", "VCB", "MWG", "PNJ", "GAS", "SAB", "MSN","VCK" , "PLX",
        "TCB", "MBB", "VPB", "SSI", "VJC", "VHM", "BID", "CTG", "STB", "NVL", "LPB", "ACB"
    };

    static {
        for (String symbol : SYMBOLS) {
            stockPrices.put(symbol, ThreadLocalRandom.current().nextDouble(20000, 100000));
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        // Giữ localhost vì chạy trong Ubuntu
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

        System.out.println("🚀 Bắt đầu sinh dữ liệu chứng khoán vào Kafka");

        for (String symbol : SYMBOLS) {
            scheduler.scheduleAtFixedRate(() -> generateAndSendTick(symbol, producer), 0, 200, TimeUnit.MILLISECONDS);
        }
    }

    private static void generateAndSendTick(String symbol, KafkaProducer<String, String> producer) {
        try {
            double currentPrice = stockPrices.get(symbol);
            double volatility = 0.002; 
            double change = 1 + (ThreadLocalRandom.current().nextGaussian() * volatility);
            double newPrice = Math.round(currentPrice * change);
            stockPrices.put(symbol, newPrice);

            double spread = ThreadLocalRandom.current().nextDouble(10, 100);
            double bid = Math.round(newPrice - spread);
            double ask = Math.round(newPrice + spread);
            int volume = ThreadLocalRandom.current().nextInt(100, 5000) * 10;
            
            // SỬA TẠI ĐÂY: Gửi nguyên Miliseconds sang Spark
            long timestamp = System.currentTimeMillis(); 

            StockTick tick = new StockTick(symbol, newPrice, volume, bid, ask, timestamp);
            String jsonValue = gson.toJson(tick);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, symbol, jsonValue);
            producer.send(record);

            System.out.println("Sent: " + jsonValue);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class StockTick {
        String symbol;
        double price;
        int volume;
        double bid;
        double ask;
        long timestamp;

        public StockTick(String symbol, double price, int volume, double bid, double ask, long timestamp) {
            this.symbol = symbol;
            this.price = price;
            this.volume = volume;
            this.bid = bid;
            this.ask = ask;
            this.timestamp = timestamp;
        }
    }
}