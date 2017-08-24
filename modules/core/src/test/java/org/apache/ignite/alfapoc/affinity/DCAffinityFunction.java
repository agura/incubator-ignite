package org.apache.ignite.alfapoc.affinity;

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Affinity function to support multi DC.
 * <p>
 * Will assign partition by {@code dc} user attribute
 */
public class DCAffinityFunction implements AffinityFunction {
    public static final int PARTITIONS_COUNT = 1024;

    private final HashingAlgorithm hashingAlgorithm;

    private final int partitions;

    public DCAffinityFunction() {
        this.partitions = PARTITIONS_COUNT;
        this.hashingAlgorithm = new MurmurAlgorithm();
    }

    public DCAffinityFunction(int partitions) {
        this.partitions = partitions;
        this.hashingAlgorithm = new MurmurAlgorithm();
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {}

    /** {@inheritDoc} */
    @Override
    public int partitions() {
        return partitions;
    }

    /** {@inheritDoc} */
    @Override
    public int partition(Object key) {
        int hash = hashingAlgorithm.hash(key);

        return hash % partitions;
    }

    /** {@inheritDoc} */
    @Override
    public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        Map<String, List<ClusterNode>> dcNodes = affCtx.currentTopologySnapshot()
                .stream()
                .filter(node -> !node.isClient())
                .collect(Collectors.groupingBy(
                        node -> node.attribute("dc")
                ));

        List<List<ClusterNode>> partitionNodes = new ArrayList<>();

        List<String> dcList = dcNodes.entrySet()
                .stream()
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList());

        for (int partition = 0; partition < partitions(); partition++) {

            List<ClusterNode> nodesForPartition = new ArrayList<>();

            for (String dc : dcList) {

                List<ClusterNode> dcLocalNodes = dcNodes.get(dc)
                        .stream()
                        .sorted(Comparator.comparingLong(ClusterNode::order))
                        .collect(Collectors.toList());


                for (int nodeIndex = 0; nodeIndex < dcLocalNodes.size(); nodeIndex++) {
                    if (partition % dcLocalNodes.size() == nodeIndex) {
                        nodesForPartition.add(dcLocalNodes.get(nodeIndex));
                    }
                }
            }

            partitionNodes.add(nodesForPartition);
        }

        return partitionNodes;
    }

    /** {@inheritDoc} */
    @Override
    public void removeNode(UUID nodeId) {}
}
