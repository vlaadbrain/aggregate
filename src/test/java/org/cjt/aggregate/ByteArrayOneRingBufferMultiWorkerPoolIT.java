package org.cjt.aggregate;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.cjt.aggregate.event.ByteArrayAggregateEvent;

import org.cjt.aggregate.simulation.DisruptorPublisher;

import org.cjt.persistence.CounterSpace;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.FatalExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WorkerPool;
import com.lmax.disruptor.YieldingWaitStrategy;

public class ByteArrayOneRingBufferMultiWorkerPoolIT {
    private final Logger log = LoggerFactory.getLogger(ByteArrayOneRingBufferMultiWorkerPoolIT.class);

    static {
        ByteArrayAggregateEvent.SIZE_HINT(64);
    }

    @Before
    public void initCounterSpace() {
        CounterSpace.instance();
    }

    @Test
    public void singlePublisherTest() {
        log.info("Start of disruptor single publisher test");
        for (int workers = 1; workers < 7; workers++) {
            ExecutorService EXECUTOR = Executors.newFixedThreadPool(workers);

            WorkerPool<ByteArrayAggregateEvent> workerPool
                = new WorkerPool<ByteArrayAggregateEvent>(
                        ByteArrayAggregateEvent.EVENT_FACTORY,
                        new SingleThreadedClaimStrategy(Common.BUFFER_SIZE),
                        new YieldingWaitStrategy(),
                        new FatalExceptionHandler(),
                        Common.getByteArrayAggregateHandlers(workers));

            RingBuffer<ByteArrayAggregateEvent> ringBuffer = workerPool.start(EXECUTOR);

            DisruptorPublisher<ByteArrayAggregateEvent> publisher
                = new DisruptorPublisher<ByteArrayAggregateEvent>(
                        ringBuffer,
                        Common.byteArrayGaussianDistribution(500000, 0.2));

            long opsPerSecond = publisher.call();

            workerPool.drainAndHalt();

            log.info("{} TPS with {} Worker/s", opsPerSecond, workers);
        }

        assertTrue(true);
    }

    @Test
    public void multiPublisherTest() {
        log.info("Start of disruptor multi publisher test");
        int workers = 2;
        for (int publishers = 1; publishers < 4; publishers++) {
            ByteArrayPublisherWithRingBuffer [] pubs = new ByteArrayPublisherWithRingBuffer[publishers];
            // create publishers and initialize them with independent gaussian distribution of data
            for (int i = 0; i < publishers; i++) {
                pubs[i] = new ByteArrayPublisherWithRingBuffer(workers, Common.byteArrayGaussianDistribution(500000, 0.2));
            }

            ExecutorService PUBLISHERS = Executors.newFixedThreadPool(publishers);
            List<Future<Long>> futures = new ArrayList<Future<Long>>();

            for (int i = 0; i < publishers; i++) {
                futures.add(PUBLISHERS.submit(pubs[i]));
            }

            for (Future<Long> future : futures) {
                try {
                    Long opsPerSecond = future.get();
                    log.info("{} TPS with {} Worker/s {} Publishers", new Object[]{opsPerSecond, workers, publishers});
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    break;
                }
            }

            for (int i = 0; i < publishers; i++) {
                pubs[i].shutdown();
            }
            PUBLISHERS.shutdown();
        }

        assertTrue(true);
    }
}

