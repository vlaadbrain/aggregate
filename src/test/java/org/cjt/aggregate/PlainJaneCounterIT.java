package org.cjt.aggregate;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.cjt.aggregate.event.PrimitiveAggregateEvent;

import org.cjt.aggregate.handler.PrimitiveAggregateHandler;

import org.cjt.persistence.CounterSpace;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainJaneCounterIT {
    private final Logger log = LoggerFactory.getLogger(PlainJaneCounterIT.class);

    @Before
    public void initCounterSpace() {
        CounterSpace.instance();
    }

    class CounterWorker implements Callable<Long> {
        PrimitiveAggregateHandler handler = new PrimitiveAggregateHandler();
        PrimitiveAggregateEvent [] events;

        public CounterWorker(PrimitiveAggregateEvent [] events) {
            this.events = events;
        }

        @Override
        public Long call() throws Exception {
            long start = System.currentTimeMillis();
            for (PrimitiveAggregateEvent event : events) {
                handler.onEvent(event);
            }
            return (events.length * 1000L) / (System.currentTimeMillis() - start);
        }
    }

    @Test
    public void noDisruptorTest() throws Exception {
        log.info("Start of CounterWorker Cassandra counter test");
        for (int workers = 1; workers < 7; workers++) {
            ExecutorService EXECUTOR = Executors.newFixedThreadPool(workers);
            CounterWorker [] WORKERS = new CounterWorker[workers];
            for (int i = 0; i < workers; i++) {
                WORKERS[i] = new CounterWorker(Common.primitiveGaussianDistribution(500000,0.2));
            }

            List<Future<Long>> futures = new ArrayList<Future<Long>>();

            for (int i = 0; i < workers; i++) {
                futures.add(EXECUTOR.submit(WORKERS[i]));
            }

            for (Future<Long> future : futures) {
                Long opsPerSecond = future.get();
                log.info("{} TPS with {} Process loop Handler/s", opsPerSecond, workers);
            }

        }

        assertTrue(true);
    }

}
