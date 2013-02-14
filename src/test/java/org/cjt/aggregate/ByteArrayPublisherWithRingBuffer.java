package org.cjt.aggregate;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.cjt.aggregate.event.ByteArrayAggregateEvent;

import org.cjt.aggregate.simulation.DisruptorPublisher;

import com.lmax.disruptor.FatalExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WorkerPool;
import com.lmax.disruptor.YieldingWaitStrategy;

public class ByteArrayPublisherWithRingBuffer implements Callable<Long> {
    DisruptorPublisher<ByteArrayAggregateEvent> publisher;
    ExecutorService EXECUTOR;
    WorkerPool<ByteArrayAggregateEvent> workerPool;
    RingBuffer<ByteArrayAggregateEvent> ringBuffer;

    public ByteArrayPublisherWithRingBuffer(int workers, ByteArrayAggregateEvent [] events) {
        EXECUTOR = Executors.newFixedThreadPool(workers);

        workerPool = new WorkerPool<ByteArrayAggregateEvent>(ByteArrayAggregateEvent.EVENT_FACTORY,
                    new SingleThreadedClaimStrategy(Common.BUFFER_SIZE),
                    new YieldingWaitStrategy(),
                    new FatalExceptionHandler(),
                    Common.getByteArrayAggregateHandlers(workers));

        ringBuffer = workerPool.start(EXECUTOR);
        this.publisher = new DisruptorPublisher<ByteArrayAggregateEvent>(ringBuffer, events);
    }

    @Override
    public Long call() {
        return publisher.call();
    }

    public void shutdown() {
        workerPool.drainAndHalt();
    }
}
