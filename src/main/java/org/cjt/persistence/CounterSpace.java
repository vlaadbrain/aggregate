package org.cjt.persistence;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.List;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.CounterRow;
import me.prettyprint.hector.api.beans.CounterRows;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.MultigetSliceCounterQuery;
import me.prettyprint.hector.api.query.QueryResult;

import me.prettyprint.hector.api.factory.HFactory;

public class CounterSpace {
    public static final String U_COLUMN_FAMILY = System.getProperty("cassandra.ucolumnFamily", "UUIDCounterFamily");
    public static final String U_COLUMN_NAME   = System.getProperty("cassandra.ucolumnName"  , "uuid_counts");
    public static final String S_COLUMN_FAMILY = System.getProperty("cassandra.scolumnFamily", "STRINGCounterFamily");
    public static final String S_COLUMN_NAME   = System.getProperty("cassandra.scolumnName"  , "string_counts");
    public static final String L_COLUMN_FAMILY = System.getProperty("cassandra.lcolumnFamily", "LONGCounterFamily");
    public static final String L_COLUMN_NAME   = System.getProperty("cassandra.lcolumnName"  , "long_counts");

    public static final String KEYSPACE_NAME   = System.getProperty("cassandra.keyspace"     , "CounterSpace");
    public static final String CLUSTER_NAME    = System.getProperty("cassandra.clusterName"  , "Test Cluster");
    public static final String CLUSTER_HOST    = System.getProperty("cassandra.clusterHost"  , "localhost");
    public static final String CLUSTER_PORT    = System.getProperty("cassandra.rpcPort"      , "9160");

    private static final UUIDSerializer uuidSerializer = new UUIDSerializer();
    private static final StringSerializer stringSerializer = new StringSerializer();
    private static final LongSerializer longSerializer = new LongSerializer();
    private static final Lock lock = new ReentrantLock();
    private static final Logger log = LoggerFactory.getLogger(CounterSpace.class);

    private static CounterSpace singletonCounterSpace;

    private Cluster cassandraCluster;
    private Keyspace keyspace;

    private void connectToCluster() throws HectorException {
        cassandraCluster = HFactory.getOrCreateCluster(CLUSTER_NAME, CLUSTER_HOST + ":" + CLUSTER_PORT);
        log.info("Connected to Cassandra cluster [{}@{}:{}]", new Object[] {CLUSTER_NAME, CLUSTER_HOST, CLUSTER_PORT});
        log.info("Thrift version: {}", cassandraCluster.describeThriftVersion());
        log.info("Cluster Partitioner: {}", cassandraCluster.describePartitioner());
    }

    private void createSchema() throws HectorException {
        log.info("Checking existing schema");
        List<KeyspaceDefinition> keyspaces = cassandraCluster.describeKeyspaces();
        if (keyspaces.size() > 0)  {
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < keyspaces.size(); i++) {
                KeyspaceDefinition kDef = keyspaces.get(i);
                buf.append(kDef.getName());
                if (i < keyspaces.size()-1)
                    buf.append(", ");
            }
            log.info("Found keyspace definitions [{}]", buf);
        }

        KeyspaceDefinition keyspaceDefinition = cassandraCluster.describeKeyspace(KEYSPACE_NAME);
        if (keyspaceDefinition != null) {
            log.info("Keyspace definition [{}] exists", KEYSPACE_NAME);
        } else {
            log.info("Creating CounterSpace keyspace [{}]", KEYSPACE_NAME);
            keyspaceDefinition = HFactory.createKeyspaceDefinition(KEYSPACE_NAME);

            cassandraCluster.addKeyspace(keyspaceDefinition, true);
            log.info("Keyspace definition [{}] created", KEYSPACE_NAME);
        }

        Set<String> cfDefs = new HashSet<String>();
        for (ColumnFamilyDefinition cfDef : keyspaceDefinition.getCfDefs())
            cfDefs.add(cfDef.getName());

        if (cfDefs.contains(U_COLUMN_FAMILY)) {
            log.info("UUID Column Family [{}] for keyspace [{}] exists", U_COLUMN_FAMILY, KEYSPACE_NAME);
        } else {
            log.info("Creating UUID Column Family [{}] for keyspace [{}]", U_COLUMN_FAMILY, KEYSPACE_NAME);
            createColumnFamily(U_COLUMN_FAMILY, ComparatorType.UTF8TYPE, "TimeUUIDType", "CounterColumnType");
        }

        if (cfDefs.contains(S_COLUMN_FAMILY)) {
            log.info("String Column Family [{}] for keyspace [{}] exists", S_COLUMN_FAMILY, KEYSPACE_NAME);
        } else {
            log.info("Creating String Column Family [{}] for keyspace [{}]", S_COLUMN_FAMILY, KEYSPACE_NAME);
            createColumnFamily(S_COLUMN_FAMILY, ComparatorType.UTF8TYPE, "UTF8Type", "CounterColumnType");
        }

        if (cfDefs.contains(L_COLUMN_FAMILY)) {
            log.info("Time Column Family [{}] for keyspace [{}] exists", L_COLUMN_FAMILY, KEYSPACE_NAME);
        } else {
            log.info("Creating Time Column Family [{}] for keyspace [{}]", L_COLUMN_FAMILY, KEYSPACE_NAME);
            createColumnFamily(L_COLUMN_FAMILY, ComparatorType.UTF8TYPE, "LongType", "CounterColumnType");
        }

        log.info("Retrieving keyspace [{}] handle", KEYSPACE_NAME);

        ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
        ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        keyspace = HFactory.createKeyspace(KEYSPACE_NAME, cassandraCluster, ccl);
        log.info("Schema successfully created!");
    }

    private void createColumnFamily(String columnFamilyName, ComparatorType comparatorType, String keyValidationClass, String defaultValidationClass) {
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE_NAME, columnFamilyName);
        cfDef.setComparatorType(comparatorType);
        cfDef.setKeyValidationClass(keyValidationClass);
        cfDef.setDefaultValidationClass(defaultValidationClass);
        cassandraCluster.addColumnFamily(cfDef, true);
    }

    private CounterSpace() throws HectorException {
        connectToCluster();
        createSchema();
    }

    public static CounterSpace instance() {
        if (singletonCounterSpace == null)  {
            lock.lock();
            try {
                if (singletonCounterSpace != null)
                    return singletonCounterSpace;
                singletonCounterSpace = new CounterSpace();
            } catch (HectorException e) {
                log.error("Unable to initialize/create [{}] on Cassandra Cluster [{}@{}:{}]", new Object[] {KEYSPACE_NAME, CLUSTER_NAME, CLUSTER_HOST, CLUSTER_PORT}, e);
                throw new RuntimeException("CounterSpace exprienced a fatal error while initializing/creating schema!", e);
            } finally {
                lock.unlock();
            }
        }
        return singletonCounterSpace;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public void incrementCounter(UUID key) throws HectorException {
        incrementCounter(key, U_COLUMN_NAME);
    }

    public void incrementCounter(UUID key, String column) throws HectorException {
        HFactory.createMutator(getKeyspace(), uuidSerializer).incrementCounter(key, U_COLUMN_FAMILY, column, 1L);
        log.debug("Increment counter column [{}] for key [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
    }

    public void incrementCounter(String key) throws HectorException {
        incrementCounter(key, S_COLUMN_NAME);
    }

    public void incrementCounter(String key, String column) throws HectorException {
        HFactory.createMutator(getKeyspace(), stringSerializer).incrementCounter(key, S_COLUMN_FAMILY, column, 1L);
        log.debug("Increment counter column [{}] for key [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
    }

    public void incrementCounter(Long key) {
        incrementCounter(key, L_COLUMN_NAME);
    }

    public void incrementCounter(Long key, String column) {
        HFactory.createMutator(getKeyspace(), longSerializer).incrementCounter(key, L_COLUMN_FAMILY, column, 1L);
        log.debug("Increment counter column [{}] for key [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
    }

    public long getCounter(UUID key) throws HectorException {
        return getCounter(key, U_COLUMN_NAME);
    }

    public long getCounter(UUID key, String column) throws HectorException {
        CounterQuery<UUID, String> counter = HFactory.createCounterColumnQuery(getKeyspace(), uuidSerializer, stringSerializer);
        counter.setColumnFamily(U_COLUMN_FAMILY).setKey(key).setName(column);
        QueryResult<HCounterColumn<String>> result = counter.execute();

        HCounterColumn<String> counterColumn = result.get();
        if (counterColumn == null) {
            log.warn("Unable to retrieve counter column [{}] key doesn't exist [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
            return 0L;
        }

        Long value = counterColumn.getValue();
        if (value == null) {
            log.warn("Unable to retrieve counter column [{}] for key [{}] has no value in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
            return 0L;
        }
        log.debug("Retrieve counter column [{}] for key [{}] in keyspace [{}]: {}", new Object[] {column, key, KEYSPACE_NAME, value});
        return value.longValue();
    }

    public long getCounter(String key) throws HectorException {
        return getCounter(key, S_COLUMN_NAME);
    }

    public long getCounter(String key, String column) throws HectorException {
        CounterQuery<String, String> counter = HFactory.createCounterColumnQuery(getKeyspace(), stringSerializer, stringSerializer);
        counter.setColumnFamily(S_COLUMN_FAMILY).setKey(key).setName(column);
        QueryResult<HCounterColumn<String>> result = counter.execute();

        HCounterColumn<String> counterColumn = result.get();
        if (counterColumn == null) {
            log.warn("Unable to retrieve counter column [{}] key doesn't exist [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
            return 0L;
        }

        Long value = counterColumn.getValue();
        if (value == null) {
            log.warn("Unable to retrieve counter column [{}] for key [{}] has no value in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
            return 0L;
        }
        log.debug("Retrieve counter column [{}] for key [{}] in keyspace [{}]: {}", new Object[] {column, key, KEYSPACE_NAME, value});
        return value.longValue();
    }

    public long getCounter(Long key) {
        return getCounter(key, L_COLUMN_NAME);
    }

    public long getCounter(Long key, String column) {
        CounterQuery<Long, String> counter = HFactory.createCounterColumnQuery(getKeyspace(), longSerializer, stringSerializer);
        counter.setColumnFamily(L_COLUMN_FAMILY).setKey(key).setName(column);
        QueryResult<HCounterColumn<String>> result = counter.execute();

        HCounterColumn<String> counterColumn = result.get();
        if (counterColumn == null) {
            log.warn("Unable to retrieve counter column [{}] key doesn't exist [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
            return 0L;
        }

        Long value = counterColumn.getValue();
        if (value == null) {
            log.warn("Unable to retrieve counter column [{}] for key [{}] has no value in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
            return 0L;
        }
        log.debug("Retrieve counter column [{}] for key [{}] in keyspace [{}]: {}", new Object[] {column, key, KEYSPACE_NAME, value});
        return value.longValue();
    }

    public long sumCounter(UUID key) throws HectorException {
        MultigetSliceCounterQuery<UUID, String> multigetSliceQuery = HFactory.createMultigetSliceCounterQuery(keyspace, uuidSerializer, stringSerializer);
        multigetSliceQuery.setColumnFamily(U_COLUMN_FAMILY);
        multigetSliceQuery.setKeys(key);
        multigetSliceQuery.setRange("", "", false, 50);
        QueryResult<CounterRows<UUID, String>> result = multigetSliceQuery.execute();
        CounterRows<UUID, String> counterRows = result.get();
        if (counterRows == null) {
            log.warn("Unable to sum counter columns because key doesn't exist [{}] in keyspace [{}]", new Object[] {key, KEYSPACE_NAME});
            return 0L;
        }

        long sum = 0L;
        for(CounterRow<UUID, String> row : counterRows)
            for (HCounterColumn<String> column : row.getColumnSlice().getColumns())
                sum += column.getValue().longValue();

        return sum;
    }

    public void dropCounter(UUID key) throws HectorException {
        dropCounter(key, U_COLUMN_NAME);
    }

    public void dropCounter(UUID key, String column) throws HectorException {
        HFactory.createMutator(getKeyspace(), uuidSerializer).deleteCounter(key, U_COLUMN_FAMILY, column, stringSerializer);
        log.debug("Drop counter column [{}] for key [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
    }

    public void dropCounter(String key) throws HectorException {
        dropCounter(key, S_COLUMN_NAME);
    }

    public void dropCounter(String key, String column) throws HectorException {
        HFactory.createMutator(getKeyspace(), stringSerializer).deleteCounter(key, S_COLUMN_FAMILY, column, stringSerializer);
        log.debug("Drop counter column [{}] for key [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
    }

    public void dropCounter(Long key) throws HectorException {
        dropCounter(key, L_COLUMN_NAME);
    }

    public void dropCounter(Long key, String column) throws HectorException {
        HFactory.createMutator(getKeyspace(), longSerializer).deleteCounter(key, L_COLUMN_FAMILY, column, stringSerializer);
        log.debug("Drop counter column [{}] for key [{}] in keyspace [{}]", new Object[] {column, key, KEYSPACE_NAME});
    }
}
