package com.nxn.kmq.example.standalone;

import com.nxn.kmq.RedeliveryTracker;
import com.nxn.kmq.example.UncaughtExceptionHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import static com.nxn.kmq.example.standalone.StandaloneConfig.KAFKA_CLIENTS;
import static com.nxn.kmq.example.standalone.StandaloneConfig.KMQ_CONFIG;

class StandaloneRedeliveryTracker {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneRedeliveryTracker.class);

    public static void main(String[] args) throws IOException {
        UncaughtExceptionHandling.setup();

        Closeable redelivery = RedeliveryTracker.start(KAFKA_CLIENTS, KMQ_CONFIG);
        LOG.info("Redelivery tracker started");

        System.in.read();

        redelivery.close();
        LOG.info("Redelivery tracker stopped");
    }
}
