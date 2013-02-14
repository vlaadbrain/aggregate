package org.cjt.persistence;

import java.util.UUID;

import org.cjt.persistence.CounterSpace;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import me.prettyprint.hector.api.factory.HFactory;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class CounterSpaceIT {
    private CounterSpace counterSpace;

    @Before
    public void setup() {
        counterSpace = CounterSpace.instance();
    }

    @After
    public void breakdown() {
        counterSpace = null;
    }

    @Test
    public void hasKeyspace() {
        assertNotNull("should've fetched a keyspace", counterSpace.getKeyspace());
    }

    @Test
    public void uuidIncrement() {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        counterSpace.incrementCounter(uuid);
        counterSpace.incrementCounter(uuid);
        counterSpace.incrementCounter(uuid);

        StringSerializer se = StringSerializer.get();
        UUIDSerializer us   = UUIDSerializer.get();
        Keyspace keyspace   = counterSpace.getKeyspace();
        String columnFamily = CounterSpace.U_COLUMN_FAMILY;
        String columnName   = CounterSpace.U_COLUMN_NAME;

        CounterQuery<UUID, String> counter = HFactory.createCounterColumnQuery(keyspace, us, se);
        counter.setColumnFamily(columnFamily);
        counter.setKey(uuid);
        counter.setName(columnName);

        QueryResult<HCounterColumn<String>> result = counter.execute(); // counter should now be 1
        assertNotNull("query result should not be null", result);

        HCounterColumn<String> counterColumn = result.get();
        assertNotNull("CounterColumn should not be null", counterColumn);

        Long value = counterColumn.getValue();
        assertEquals("should've incremented counter by 3", 3, value.longValue());
    }

    @Test
    public void uuidIncrementAndGet() {
        UUID key = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);

        assertEquals("should've incremented counter by 3", 3, counterSpace.getCounter(key));

        String column = "asdfdsa";
        counterSpace.incrementCounter(key, column);
        counterSpace.incrementCounter(key, column);
        counterSpace.incrementCounter(key, column);

        assertEquals("should've incremented counter by 3", 3, counterSpace.getCounter(key, column));

        assertEquals("Should've summed up all counters for key", 6, counterSpace.sumCounter(key));
    }

    @Test
    public void stringIncrement() {
        String key = "This is my special key";
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);

        StringSerializer se = StringSerializer.get();
        Keyspace keyspace   = counterSpace.getKeyspace();
        String columnFamily = CounterSpace.S_COLUMN_FAMILY;
        String columnName   = CounterSpace.S_COLUMN_NAME;

        CounterQuery<String, String> counter = HFactory.createCounterColumnQuery(keyspace, se, se);
        counter.setColumnFamily(columnFamily);
        counter.setKey(key);
        counter.setName(columnName);

        QueryResult<HCounterColumn<String>> result = counter.execute(); // counter should now be 1
        assertNotNull("query result should not be null", result);

        HCounterColumn<String> counterColumn = result.get();
        assertNotNull("CounterColumn should not be null", counterColumn);

        Long value = counterColumn.getValue();
        assertEquals("should've incremented counter by 3", 3, value.longValue());
    }

    @Test
    public void stringIncrementAndGet() {
        String key = "This is another special key!" + TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString();
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);

        assertEquals("should've incremented counter by 3", 3, counterSpace.getCounter(key));

        String column = "Tsthdhr";

        counterSpace.incrementCounter(key, column);
        counterSpace.incrementCounter(key, column);
        counterSpace.incrementCounter(key, column);
        counterSpace.incrementCounter(key, column);

        assertEquals("should've incremented counter by 4", 4, counterSpace.getCounter(key, column));
    }

    @Test
    public void longIncrement() {
        Long key = 123456789L;
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);

        StringSerializer se = StringSerializer.get();
        LongSerializer le = LongSerializer.get();
        Keyspace keyspace   = counterSpace.getKeyspace();
        String columnFamily = CounterSpace.L_COLUMN_FAMILY;
        String columnName   = CounterSpace.L_COLUMN_NAME;

        CounterQuery<Long, String> counter = HFactory.createCounterColumnQuery(keyspace, le, se);
        counter.setColumnFamily(columnFamily);
        counter.setKey(key);
        counter.setName(columnName);

        QueryResult<HCounterColumn<String>> result = counter.execute(); // counter should now be 1
        assertNotNull("query result should not be null", result);

        HCounterColumn<String> counterColumn = result.get();
        assertNotNull("CounterColumn should not be null", counterColumn);

        Long value = counterColumn.getValue();
        assertEquals("should've incremented counter by 3", 3, value.longValue());
    }

    @Test
    public void incrementCounterWithLongTypeKeyAndStaticColumnName() {
        long key = 10000L;
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);
        counterSpace.incrementCounter(key);

        assertEquals("should've incremented counter by 3", 3, counterSpace.getCounter(key));

        String column = "This is yet another key" +  TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString();
        counterSpace.incrementCounter(key, column);
        counterSpace.incrementCounter(key, column);
        counterSpace.incrementCounter(key, column);

        assertEquals("should've incremented counter by 3", 3, counterSpace.getCounter(key, column));
    }

    @Test
    public void getBlank() {
        UUID key = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        assertEquals("should've returned 0", 0, counterSpace.getCounter(key));
    }

    @Test
    public void canDeleteKey() {
        UUID key = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        counterSpace.incrementCounter(key);
        assertEquals("incremented uuid accordingly", 1, counterSpace.getCounter(key));
        counterSpace.dropCounter(key);
        assertEquals("should've returned 0 because i dropped it", 0, counterSpace.getCounter(key));
    }

    @Test
    public void canDropNonExistentRow() {
        try {
            UUID key = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
            counterSpace.dropCounter(key);
        } catch(Exception e) {
            fail("should be able to drop non-existent rows");
        }
    }
}
