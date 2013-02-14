package org.cjt.aggregate.event;

import java.io.UnsupportedEncodingException;

import java.lang.reflect.Field;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public class ByteArrayAggregateEventTest {
    @After
    public void breakdown() throws IllegalAccessException, NoSuchFieldException {
        // not sure if this is a smell, but i really think the size of the bytes array in 
        // ByteArrayAggregateEvent needs to be in the class. i expect this code to be used 
        // for one purpose.
        Field hinted= ByteArrayAggregateEvent.class.getDeclaredField("hinted");
        hinted.setAccessible(true);
        hinted.set(null, false);

        Field SIZE_HINT= ByteArrayAggregateEvent.class.getDeclaredField("SIZE_HINT");
        SIZE_HINT.setAccessible(true);
        SIZE_HINT.set(null, 128);
    }

    @Test
    public void instanceValid() {
        ByteArrayAggregateEvent byteArrayAggregateEvent = ByteArrayAggregateEvent.EVENT_FACTORY.newInstance();
        assertNotNull("should be a valid instance", byteArrayAggregateEvent);
        assertTrue("should be a valid instance of PrimitiveAggregateEvent", byteArrayAggregateEvent instanceof ByteArrayAggregateEvent);
    }

    @Test
    public void ignoreSizeHintAfterFirstConstruction() {
        @SuppressWarnings("unused")
		ByteArrayAggregateEvent event = ByteArrayAggregateEvent.EVENT_FACTORY.newInstance();

        ByteArrayAggregateEvent.SIZE_HINT(1024);

        assertEquals("should have ignored that change", 128, ByteArrayAggregateEvent.EVENT_FACTORY.newInstance().length());
    }

    @Test
    public void takesSizeHint() {
        int hint = 256;
        int largerHint = 512;
        ByteArrayAggregateEvent.SIZE_HINT(hint);
        assertEquals("should take size hint", hint, ByteArrayAggregateEvent.EVENT_FACTORY.newInstance().length());
        
        ByteArrayAggregateEvent.SIZE_HINT(largerHint);
        assertEquals("should ignore additional size_hint calls", hint, ByteArrayAggregateEvent.EVENT_FACTORY.newInstance().length());
    }

    @Test
    public void truncatesLongerBytesSet() throws UnsupportedEncodingException {
        ByteArrayAggregateEvent.SIZE_HINT(5);
        ByteArrayAggregateEvent byteArrayAggregateEvent = ByteArrayAggregateEvent.EVENT_FACTORY.newInstance();
        byteArrayAggregateEvent.setBytes(("HelloWorld").getBytes("UTF8"));
        assertEquals("should be Hello", "Hello" , new String(byteArrayAggregateEvent.getBytes(), "UTF8"));
    }

    @Test
    public void padsShorterBytesSet() throws UnsupportedEncodingException {
        ByteArrayAggregateEvent.SIZE_HINT(10);
        ByteArrayAggregateEvent byteArrayAggregateEvent = ByteArrayAggregateEvent.EVENT_FACTORY.newInstance();
        byteArrayAggregateEvent.setBytes(("Hello").getBytes("UTF8"));
        assertEquals("should be Hello with padding (5 spaces)", "Hello     " , new String(byteArrayAggregateEvent.getBytes(), "UTF8"));
    }

    @Test
    public void copiesByteSetOfEqualLength() throws UnsupportedEncodingException {
        ByteArrayAggregateEvent.SIZE_HINT(10);
        ByteArrayAggregateEvent byteArrayAggregateEvent = ByteArrayAggregateEvent.EVENT_FACTORY.newInstance();
        byteArrayAggregateEvent.setBytes(("HelloWorld").getBytes("UTF8"));
        assertEquals("should be HelloWorld", "HelloWorld" , new String(byteArrayAggregateEvent.getBytes(), "UTF8"));
    }
}
