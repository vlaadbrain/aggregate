package org.cjt.aggregate.event;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lmax.disruptor.EventFactory;

/**
 * ByteArrayAggregateEvent hosts criteria that is aggregated concurrently in CounterStore
 * - the byte array needs to be preallocated and the size cannot be hinted at in the constructor
 *   because the EventFactory interface doesn't allow for argument passing to the constructor.
 * - We need default constructor for EVENT_FACTORY so that disruptor can "pre-populate"
 *   cache with ByteArrayAggregateEvents and keep the cache warm.
 * - we want the disruptor to work from cache; its faster!!
 *   http://disruptor.googlecode.com/files/Disruptor-1.0.pdf
 * - the RingBuffer will be pre-populated by Events of this type in an array using the default constructor.
 *   The size of each event matters, as it affects how it lines up in the cache for the CORE the thread
 *   will run on.  to avoid cache misses and allow cache preemption the size of the byte array must be
 *   predetermined and initialized before creating the ring-buffer.
 * - a static SIZE_HINT(int size) method is provided to manipulate the size at runtime prior
 *   to the creation of the ring-buffer.
 * - calling SIZE_HINT more than once is ignored.
 * - calling SIZE_HINT after the first call to a constructor is also ignored.
 * - set methods will only copy the from the array passed the length of the SIZE_HINT set and
 *   will ignore the rest, carefull use of length() should be done.
 */

public final class ByteArrayAggregateEvent implements Copyable<ByteArrayAggregateEvent> {
    public static int SIZE_HINT = 128;
    private static boolean hinted = false;
    private static final Lock lock = new ReentrantLock();
    private static byte [] padding;

    public static void SIZE_HINT(int hint) {
        if (hinted) return;
        lock.lock();
        try {
            if (hinted) return;
            SIZE_HINT = hint;
            hinted = true;
            createPadding();
        } finally {
            lock.unlock();
        }
    }

    private static void createPadding() {
        padding = new byte[SIZE_HINT];
        for (int i = 0; i < SIZE_HINT; i++)
            padding[i] = ' ';
    }

    private final byte [] bytes;
    private long timeInMillis;

    public ByteArrayAggregateEvent() {
        SIZE_HINT(SIZE_HINT);
        bytes = new byte[SIZE_HINT];
    }

    public static EventFactory<ByteArrayAggregateEvent> EVENT_FACTORY = new EventFactory<ByteArrayAggregateEvent>() {
        @Override
        public ByteArrayAggregateEvent newInstance() {
            return new ByteArrayAggregateEvent();
        }
    };

    /**
     * Gets the bytes for this instance.
     *
     * @return The bytes.
     */
    public byte[] getBytes() {
        return this.bytes;
    }

    /**
     * Returns the length of the byte array
     *
     * @return the length of the byte array
     */
    public int length() {
        return this.bytes.length;
    }

    /**
     * Sets the bytes for this instance.
     *
     * @param bytes The bytes.
     */
    public void setBytes(byte[] src) {
        /*
         *  ,-----------------,---- malloced bytes
         * [0][1][2][3][4][5][6] <- index
         * [1][2][3][4][5][6][7] <- this.length()
         *        ^---------------- src.length
         *           ^--------^---- padding
         */
        if (src.length < this.length()) {
            System.arraycopy(src, 0, bytes, 0, src.length);
            System.arraycopy(padding, src.length, bytes, src.length, this.length() - src.length);
        } else {
            System.arraycopy(src, 0, bytes, 0, this.length());
        }
    }

    /**
     * Gets the timeInMillis for this instance.
     *
     * @return The timeInMillis.
     */
    public long getTimeInMillis() {
        return this.timeInMillis;
    }

    /**
     * Sets the timeInMillis for this instance.
     *
     * @param timeInMillis The timeInMillis.
     */
    public void setTimeInMillis(long timeInMillis) {
        this.timeInMillis = timeInMillis;
    }

    @Override
    public void copy(ByteArrayAggregateEvent copy) {
        this.setBytes(copy.getBytes());
        this.setTimeInMillis(copy.getTimeInMillis());
    }
}
