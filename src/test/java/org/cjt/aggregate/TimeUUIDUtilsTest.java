package org.cjt.aggregate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.service.clock.MicrosecondsSyncClockResolution;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import me.prettyprint.hector.api.ClockResolution;

public class TimeUUIDUtilsTest {

    private static final Logger log = LoggerFactory.getLogger(TimeUUIDUtilsTest.class);

    class RowInserter implements Callable<Set<UUID>> {
        private final int insertCount;
        private final ClockResolution clockRes;

        public RowInserter(ClockResolution clockRes, int insertCount) {
            this.insertCount = insertCount;
            this.clockRes = clockRes;
        }

        public Set<UUID> call() {
            Set<UUID> set = new HashSet<UUID>();
            for (int count = 0;count < insertCount; count++) {
                UUID myKey = TimeUUIDUtils.getTimeUUID(clockRes.createClock());
                set.add(myKey);
            }
            return set;
        }
    }

    @Test
    public void testConcurrentTimeUUIDuniqueness() {
        int nThreads = 5;
        int nRowInserters = 1000;
        int nInsertsPerRI = 1000;

        int totalExpected = nRowInserters * nInsertsPerRI;
        log.info("Test Concurrent TimeUUID uniqueness, with {} UUIDS with {} threads and one ClockResolution",
                totalExpected, nThreads);
        ExecutorService exec = Executors.newFixedThreadPool(nThreads);
        MicrosecondsSyncClockResolution clockRes = new MicrosecondsSyncClockResolution();

        List<Future<Set<UUID>>> futures = new ArrayList<Future<Set<UUID>>>();

        log.info("Loading ExecutorService with {} RowInserters", nRowInserters);
        for (int x=0; x<nRowInserters; x++ ) {
            futures.add(exec.submit(new TimeUUIDUtilsTest().new RowInserter(clockRes, nInsertsPerRI)));
        }

        Set<UUID> resultSet = new HashSet<UUID>();
        try {
            for ( Future<Set<UUID>> f : futures ) {
                resultSet.addAll(f.get());
            }
        } catch (Exception ex) {
            log.error("woops failed to gather all the Futures in the service", ex);
        }

        log.info("done executing all RowInserters");
        assertEquals("there should've been " + totalExpected +" unique keys", totalExpected, resultSet.size());
        exec.shutdown();
        log.info("Expected [{}] and found [{}] unique keys UUIDs",
                new Object[] { totalExpected, resultSet.size()} );
        log.info("ExecutorService shutdown");
    }
}
