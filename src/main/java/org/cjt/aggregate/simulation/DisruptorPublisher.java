package org.cjt.aggregate.simulation;

import java.util.concurrent.Callable;

import org.cjt.aggregate.event.Copyable;

import com.lmax.disruptor.RingBuffer;

public class DisruptorPublisher<E extends Copyable<E>> implements Callable<Long> {
    private RingBuffer<E> ringBuffer;
    private E[] events;

    public DisruptorPublisher(RingBuffer<E> ringBuffer, E[] events) {
        this.ringBuffer = ringBuffer;
        this.events = events;
    }

    @Override
    public Long call() {
        long start = System.currentTimeMillis();

        for (E e : events) {
            long sequence = ringBuffer.next();
            E event = ringBuffer.get(sequence);
            event.copy(e);
            ringBuffer.publish(sequence);
        }

        long opsPerSecond = (events.length * 1000L) / (System.currentTimeMillis() - start);
        return opsPerSecond;
    }
}
