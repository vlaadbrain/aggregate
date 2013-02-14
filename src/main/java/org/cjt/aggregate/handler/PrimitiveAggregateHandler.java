package org.cjt.aggregate.handler;

import org.cjt.aggregate.event.PrimitiveAggregateEvent;

import org.cjt.persistence.CounterSpace;

import com.lmax.disruptor.WorkHandler;

public class PrimitiveAggregateHandler implements WorkHandler<PrimitiveAggregateEvent> {
    private final static String format = "%d-%d-%f-%d";

	@Override
	public void onEvent(PrimitiveAggregateEvent event) throws Exception {
        CounterSpace.instance().incrementCounter(
                String.format(format,
                    event.getId1(), 
                    event.getId2(), 
                    event.getId3(), 
                    event.getTimeInMillis()));
	}
}
