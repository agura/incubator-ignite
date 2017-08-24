package org.apache.ignite.alfapoc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.alfapoc.affinity.DCAffinityFunction;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.HashMap;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

public class AlfaTest extends GridCommonAbstractTest {
    private static final String POJOS_CACHE = "pojosCacheConfiguration";
    private static final int NODES_CNT = 4;

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(EVT_CACHE_REBALANCE_PART_DATA_LOST);

        TransactionConfiguration txCfg = new TransactionConfiguration();
        txCfg.setDefaultTxConcurrency(TransactionConcurrency.PESSIMISTIC);
        txCfg.setDefaultTxIsolation(TransactionIsolation.REPEATABLE_READ);
        cfg.setTransactionConfiguration(txCfg);

        final int num = Integer.valueOf(igniteInstanceName.substring(igniteInstanceName.length() - 1));

        //TODO: make it specific for nodes
        cfg.setUserAttributes(new HashMap<String, Object>() {{
            put("dc", num > 1 ? "dc2" : "dc1");
        }});


        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();
        memPlcCfg.setName("ordersMemoryPolicy");

        MemoryConfiguration memCfg = new MemoryConfiguration();
        memCfg.setMemoryPolicies(memPlcCfg);

        cfg.setMemoryConfiguration(memCfg);

        cfg.setCacheConfiguration(pojosCacheConfiguration());

        return cfg;
    }

    public CacheConfiguration<Long, String> pojosCacheConfiguration() {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(POJOS_CACHE);

        ccfg.setBackups(1);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setCopyOnRead(true);
        ccfg.setReadFromBackup(true);
        ccfg.setStatisticsEnabled(false);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setQueryParallelism(3);
        //ccfg.setRebalanceMode(CacheRebalanceMode.NONE);
        ccfg.setMemoryPolicyName("ordersMemoryPolicy");
        //ccfg.setOnheapCacheEnabled(true);
        ccfg.setAffinity(new DCAffinityFunction());
        //ccfg.setAffinity(new DCAffinityFunction(8));

        //ccfg.setAffinity(new RendezvousAffinityFunction());

        //ccfg.setIndexedTypes(Long.class, String.class);

        return ccfg;
    }

    public void testDataLose() throws Exception {
        startGridsMultiThreaded(NODES_CNT);

        rebalance(ignite(0));

        for (int i = 0; i < NODES_CNT; i++)
            listenPartLostEvt(ignite(i));

        Ignite prmrNode = grid(0);

        IgniteCache<Long, String> prmrCache = prmrNode.cache(POJOS_CACHE);

        for (int i = 0; i < 8192; i++) {
            prmrCache.put((long)i, "Entry_" + i);
        }

        logSize(ignite(0)); // 4

        stopGrid(0);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(1));
        rebalance(ignite(2));
        rebalance(ignite(3));
*/
        logSize(ignite(2)); // 5

        stopGrid(1);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(2));
        rebalance(ignite(3));
*/
        logSize(ignite(2)); // 6

        startGrid(0);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(0)); // 7
        rebalance(ignite(2));
        rebalance(ignite(3));
*/
        logSize(ignite(0));

//        U.sleep(5000);

        startGrid(1);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(0)); // 8
        rebalance(ignite(1));
        rebalance(ignite(2));
        rebalance(ignite(3));
*/
        logSize(ignite(2));

        stopGrid(2);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(0)); // 9
        rebalance(ignite(1));
        rebalance(ignite(3));
*/
        logSize(ignite(0));

        stopGrid(3);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(0)); // 10
        rebalance(ignite(1));
*/
        logSize(ignite(0));

        startGrid(2);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(0)); // 11
        rebalance(ignite(1));
        rebalance(ignite(2));
*/
        logSize(ignite(0));

        startGrid(3);

/*
        awaitPartitionMapExchange();
        rebalance(ignite(0)); // 12
        rebalance(ignite(1));
        rebalance(ignite(2));
        rebalance(ignite(3));
*/
        logSize(ignite(0));

        log.info("!!! Test end");

        stopAllGrids();
    }

    private void logSize(Ignite ignite) {
        log.info("!!! Size: " + ignite.cache(POJOS_CACHE).size() +
                ", topVer = " + ((IgniteEx)ignite).cluster().topologyVersion());
    }

    private void rebalance(Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.cache(POJOS_CACHE);

        CacheConfiguration ccfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        if (ccfg.getRebalanceMode() == CacheRebalanceMode.NONE) {
            log.info("!!! Rebalance started");

            cache.rebalance().get();

            log.info("!!! Rebalance finished");
        }
    }

    @Override
    protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx ignite = super.startGrid(idx);

        listenPartLostEvt(ignite);

        return ignite;
    }

    private void listenPartLostEvt(Ignite ignite) {
        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(Event evt) {
                System.out.println("!!! Lost partition: " + evt);

                return true;
            }
        }, EVT_CACHE_REBALANCE_PART_DATA_LOST);
    }
}
