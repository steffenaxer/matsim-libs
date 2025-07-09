package org.matsim.contrib.drt.optimizer.insertion.parallel;

import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.requests.PartitionScalingFunction;

/**
 * @author steffenaxer
 */
public class ParallelInserterUtils {

	/**
	 * Calculates the number of active partitions as a function of totalNumberOfPartitions,
	 * and request density per minute. This scaling function adds for every additional 20 rides/minute
	 * an additional partition. Each partition is managed by an own thread.
	 */
	public static PartitionScalingFunction getDefaultPartitionScalingFunction() {
		return (totalNumberOfPartitions, requests, period) -> {
			double requestsPerMinute = requests * (60.0 / period);
			return Math.min(totalNumberOfPartitions, Math.max(1, (int) (requestsPerMinute / 20) + 1));
		};
	}
}
