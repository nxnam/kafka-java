package com.nxn.kmq;

import com.nxn.kmq.redelivery.RedeliveryActors;

import scala.Option;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

public class RedeliveryTracker {
    public static Closeable start(KafkaClients clients, KmqConfig config) {
        return RedeliveryActors.start(clients, config);
    }
}
