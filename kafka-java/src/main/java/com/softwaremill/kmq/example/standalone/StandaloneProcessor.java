package com.nxn.kmq.example.standalone;

import com.nxn.kmq.KmqClient;
import com.nxn.kmq.example.UncaughtExceptionHandling;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.nxn.kmq.example.standalone.StandaloneConfig.KAFKA_CLIENTS;
import static com.nxn.kmq.example.standalone.StandaloneConfig.KMQ_CONFIG;

class StandaloneProcessor {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneProcessor.class);

    public static void main(String[] args) {
        UncaughtExceptionHandling.setup();

        KmqClient<ByteBuffer, ByteBuffer> kmqClient = new KmqClient<>(KMQ_CONFIG, KAFKA_CLIENTS,
                ByteBufferDeserializer.class, ByteBufferDeserializer.class, Duration.ofMillis(100));

        ExecutorService msgProcessingExecutor = Executors.newCachedThreadPool();

        while (true) {
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : kmqClient.nextBatch()) {
                msgProcessingExecutor.execute(() -> {
                    if (processMessage(record)) {
                        kmqClient.processed(record);
                    }
                });
            }
        }
    }

    private static Random random = new Random();
    private static Map<Integer, Integer> processedMessages = new ConcurrentHashMap<>();
    private static AtomicInteger totalProcessed = new AtomicInteger(0);
    private static boolean processMessage(ConsumerRecord<ByteBuffer, ByteBuffer> rawMsg) {
        int msg = rawMsg.value().getInt();
        // 10% of the messages are dropped
        if (random.nextInt(10) != 0) {
            // Sleeping up to 2.5 seconds
            LOG.info("Processing message: " + msg);
            try {
                Thread.sleep(random.nextInt(25)*100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer previous = processedMessages.put(msg, msg);
            if (previous != null) {
                LOG.warn(String.format("Message %d was already processed!", msg));
            }

            int total = totalProcessed.incrementAndGet();
            LOG.info(String.format("Done processing message: %d. Total processed: %d.", msg, total));

            return true;
        } else {
            LOG.info("Dropping message: " + msg);
            return false;
        }
    }
}
