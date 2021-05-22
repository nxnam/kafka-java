package com.nxn.kmq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

public class KmqClient<K, V> implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(KmqClient.class);

    private final KmqConfig config;
    private final Duration msgPollTimeout;

    private final KafkaConsumer<K, V> msgConsumer;
    private final KafkaProducer<MarkerKey, MarkerValue> markerProducer;

    public KmqClient(KmqConfig config, KafkaClients clients,
                     Class<? extends Deserializer<K>> keyDeserializer,
                     Class<? extends Deserializer<V>> valueDeserializer,
                     Duration msgPollTimeout) {

        this.config = config;
        this.msgPollTimeout = msgPollTimeout;

        this.msgConsumer = clients.createConsumer(config.getMsgConsumerGroupId(), keyDeserializer, valueDeserializer);
        this.markerProducer = clients.createProducer(
                MarkerKey.MarkerKeySerializer.class, MarkerValue.MarkerValueSerializer.class,
                Collections.singletonMap(ProducerConfig.PARTITIONER_CLASS_CONFIG, ParititionFromMarkerKey.class));

        LOG.info(String.format("Subscribing to topic: %s, using group id: %s", config.getMsgTopic(), config.getMsgConsumerGroupId()));
        msgConsumer.subscribe(Collections.singletonList(config.getMsgTopic()));
    }

    public ConsumerRecords<K, V> nextBatch() {
        List<Future<RecordMetadata>> markerSends = new ArrayList<>();

        ConsumerRecords<K, V> records = msgConsumer.poll(msgPollTimeout);
        for (ConsumerRecord<K, V> record : records) {
            markerSends.add(markerProducer.send(
                    new ProducerRecord<>(config.getMarkerTopic(),
                            MarkerKey.fromRecord(record),
                            new StartMarker(config.getMsgTimeoutMs()))));
        }

        markerSends.forEach(f -> {
            try { f.get(); } catch (Exception e) { throw new RuntimeException(e); }
        });

        msgConsumer.commitSync();

        return records;
    }

    public Future<RecordMetadata> processed(ConsumerRecord<K, V> record) {
        return markerProducer.send(new ProducerRecord<>(config.getMarkerTopic(),
                MarkerKey.fromRecord(record),
                EndMarker.INSTANCE));
    }

    @Override
    public void close() throws IOException {
        msgConsumer.close();
        markerProducer.close();
    }
}
