package org.cjt.aggregate;

import java.util.Random;

import org.cjt.aggregate.event.ByteArrayAggregateEvent;
import org.cjt.aggregate.event.PrimitiveAggregateEvent;

import org.cjt.aggregate.handler.ByteArrayAggregateHandler;
import org.cjt.aggregate.handler.PrimitiveAggregateHandler;

public class Common {
    public static final int BUFFER_SIZE = 1024 * 8;
    public static final int ITERATIONS = 100 * 1000;

    public static PrimitiveAggregateHandler [] getPrimitiveAggregateHandlers(int count) {
        PrimitiveAggregateHandler [] handlers = new PrimitiveAggregateHandler[count];

        for (int i = 0; i < count; i++) {
            handlers[i] = new PrimitiveAggregateHandler();
        }

        return handlers;
    }

    public static PrimitiveAggregateEvent [] primitiveGaussianDistribution(double MEAN, double VARIANCE) {
        Random random = new Random();
        PrimitiveAggregateEvent [] events = new PrimitiveAggregateEvent[ITERATIONS];

        for (int i = 0; i < ITERATIONS; i++) {
            double value = MEAN + random.nextGaussian() * VARIANCE;

            events[i] = new PrimitiveAggregateEvent();
            events[i].setId1(123);
            events[i].setId2(456);
            events[i].setId3(value);
            events[i].setTimeInMillis((System.currentTimeMillis()/1000L)*1000L);
        }
        return events;
    }

    //public static PrimitiveAggregateEvent [] logNormalDistribution(int quantity) {
    //}

    public static ByteArrayAggregateHandler [] getByteArrayAggregateHandlers(int count) {
        ByteArrayAggregateHandler [] handlers = new ByteArrayAggregateHandler[count];

        for (int i = 0; i < count; i++) {
            handlers[i] = new ByteArrayAggregateHandler();
        }

        return handlers;
    }

    public static ByteArrayAggregateEvent [] byteArrayGaussianDistribution(double MEAN, double VARIANCE) {
        Random random = new Random();
        ByteArrayAggregateEvent [] events = new ByteArrayAggregateEvent[ITERATIONS];

        for (int i = 0; i < ITERATIONS; i++) {
            double value = MEAN + random.nextGaussian() * VARIANCE;
            events[i] = new ByteArrayAggregateEvent();
            events[i].setBytes(Double.toString(value).getBytes());
        }

        return events;
    }
}
