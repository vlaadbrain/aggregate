package org.cjt.aggregate;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.cjt.aggregate.event.PrimitiveAggregateEvent;

import org.cjt.aggregate.simulation.DisruptorPublisher;

import com.lmax.disruptor.FatalExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WorkerPool;
import com.lmax.disruptor.YieldingWaitStrategy;

public class PrimitivePublisherWithRingBuffer implements Callable<Long> {
    DisruptorPublisher<PrimitiveAggregateEvent> publisher;
    ExecutorService EXECUTOR;
    WorkerPool<PrimitiveAggregateEvent> workerPool;
    RingBuffer<PrimitiveAggregateEvent> ringBuffer;

    public PrimitivePublisherWithRingBuffer(int workers, PrimitiveAggregateEvent [] events) {
        EXECUTOR = Executors.newFixedThreadPool(workers);

        workerPool = new WorkerPool<PrimitiveAggregateEvent>(PrimitiveAggregateEvent.EVENT_FACTORY,
                    new SingleThreadedClaimStrategy(Common.BUFFER_SIZE),
                    new YieldingWaitStrategy(),
                    new FatalExceptionHandler(),
                    Common.getPrimitiveAggregateHandlers(workers));

        ringBuffer = workerPool.start(EXECUTOR);
        this.publisher = new DisruptorPublisher<PrimitiveAggregateEvent>(ringBuffer, events);
    }

    @Override
    public Long call() {
        return publisher.call();
    }

    public void shutdown() {
        workerPool.drainAndHalt();
    }
}
