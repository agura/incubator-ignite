package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class ClusterSyncOnWalTest extends GridCommonAbstractTest {
    /** Nodes. */
    public static final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalSegmentSize(512 * 1024)
                .setCheckpointFrequency(9_000)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(200L * 1024 * 1024)
                        .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /**
     *
     */
    public void test() throws Exception {
        System.setProperty("IGNITE_WAL_MMAP", "false");

        IgniteEx ignite0 = (IgniteEx)startGrids(NODES);

        ignite0.cluster().active(true);

        assertEquals(ignite0.configuration().getDataStorageConfiguration().getWalSegmentSize(), 512 * 1024);

        try (final IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);
            Random rand = new Random();

            for (long iter = 0; ; iter++) {
                IgniteInternalFuture future = GridTestUtils.runAsync(() -> {
                    for (int i = 0; i < 100_000; i++)
                        streamer.addData(i, String.valueOf(rand.nextLong()));

                    streamer.flush();
                });

                try {
                    future.get(100_000);
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    printAllStackTraces();

                    fail("Fail on iteration " + iter);

                }

                if (iter % 100 == 0)
                    info("Coplete load on iter " + iter);
            }
        }
    }

    /**
     *
     */
    private static void printAllStackTraces() {
        Map liveThreads = Thread.getAllStackTraces();

        for (Iterator i = liveThreads.keySet().iterator(); i.hasNext(); ) {
            Thread key = (Thread)i.next();

            System.err.println("Thread " + key.getName());

            StackTraceElement[] trace = (StackTraceElement[])liveThreads.get(key);

            for (int j = 0; j < trace.length; j++)
                System.err.println("\tat " + trace[j]);
        }
    }
}
