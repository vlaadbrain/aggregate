package org.cjt.aggregate.event;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PrimitiveAggregateEventTest {
    private PrimitiveAggregateEvent aggregateEvent;

    @Before
    public void setup() {
        aggregateEvent = PrimitiveAggregateEvent.EVENT_FACTORY.newInstance();
    }

    @After
    public void breakdown() {
        aggregateEvent = null;
    }

    @Test
    public void instanceValid() {
        assertNotNull("should be a valid instance", aggregateEvent);
        assertTrue("should be a valid instance of PrimitiveAggregateEvent", aggregateEvent instanceof PrimitiveAggregateEvent);
    }
}
