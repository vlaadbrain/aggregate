package org.cjt.aggregate.event;

import com.lmax.disruptor.EventFactory;

public final class PrimitiveAggregateEvent implements Copyable<PrimitiveAggregateEvent> {
    private long id1;
    private long id2;
    private double id3;
    private long timeInMillis;

    /**
     * AggregateEvent hosts criteria that is aggregated concurrently in CounterStore
     * - need default constructor for EVENT_FACTORY so that disruptor can "pre-populate"
     *   cache with AggregateEvents and keep the cache warm.
     * - we want the disruptor to work from cache; its faster!!
     */
    public PrimitiveAggregateEvent() {}


    public static  EventFactory<PrimitiveAggregateEvent> EVENT_FACTORY = new EventFactory<PrimitiveAggregateEvent>() {
        @Override
        public PrimitiveAggregateEvent newInstance() {
            return new PrimitiveAggregateEvent();
        }
    };

    /**
     * Gets the id1 for this instance.
     *
     * @return The id1.
     */
    public long getId1() {
        return this.id1;
    }

    /**
     * Sets the id1 for this instance.
     *
     * @param id1 The id1.
     */
    public void setId1(long id1) {
        this.id1 = id1;
    }

    /**
     * Gets the id2 for this instance.
     *
     * @return The id2.
     */
    public long getId2() {
        return this.id2;
    }

    /**
     * Sets the id2 for this instance.
     *
     * @param id2 The id2.
     */
    public void setId2(long id2) {
        this.id2 = id2;
    }

    /**
     * Gets the id3 for this instance.
     *
     * @return The id3.
     */
    public double getId3() {
        return this.id3;
    }

    /**
     * Sets the id3 for this instance.
     *
     * @param id3 The id3.
     */
    public void setId3(double id3) {
        this.id3 = id3;
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
    public void copy(PrimitiveAggregateEvent copy) {
        this.setId1(copy.getId1());
        this.setId2(copy.getId2());
        this.setId3(copy.getId3());
        this.setTimeInMillis(copy.getTimeInMillis());
    }
}
