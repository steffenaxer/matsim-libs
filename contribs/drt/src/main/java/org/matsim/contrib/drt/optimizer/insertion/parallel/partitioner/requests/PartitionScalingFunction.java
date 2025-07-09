package org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.requests;

@FunctionalInterface
public interface PartitionScalingFunction {
    int computeActivePartitions(int totalPartitions, int requestCount, double collectionPeriod);
}
