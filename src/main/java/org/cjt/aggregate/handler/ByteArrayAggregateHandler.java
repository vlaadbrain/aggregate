package org.cjt.aggregate.handler;

import java.util.UUID;

import java.util.concurrent.TimeUnit;

import org.cjt.aggregate.event.ByteArrayAggregateEvent;

import org.cjt.persistence.CounterSpace;

import com.lmax.disruptor.WorkHandler;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

public class ByteArrayAggregateHandler implements WorkHandler<ByteArrayAggregateEvent> {
    private final TimeUnit desiredResolution;

    public ByteArrayAggregateHandler() {
        this(TimeUnit.MINUTES);
    }

    public ByteArrayAggregateHandler(TimeUnit res) {
        this.desiredResolution = res;
    }

    @Override
    public void onEvent(ByteArrayAggregateEvent event) throws Exception {
        String key = new String(event.getBytes(), "UTF8");
        UUID uuid = TimeUUIDUtils.getTimeUUID(deRez(event.getTimeInMillis()));
        CounterSpace.instance().incrementCounter(uuid, key);
    }

    private long deRez(long milliseconds) {
        long res = desiredResolution.convert(milliseconds, TimeUnit.MILLISECONDS);
        return TimeUnit.MILLISECONDS.convert(res, desiredResolution);
    }
}
