package org.matsim.contrib.drt.optimizer.insertion.parallel;

import org.matsim.core.config.ReflectiveConfigGroup;


public class ParallelInserterParams extends ReflectiveConfigGroup {
	enum VehiclePartitioner {
		ReplicatingVehicleEntryPartitioner, RoundRobinVehicleEntryPartitioner,
		ShiftingRoundRobinVehicleEntryPartitioner
	}

	enum RequestPartitioner {RoundRobinRequestsPartitioner, LoadAwareRoundRobinRequestsPartitioner,}

	public static final String GROUP_NAME = "parallelInserter";

	@Parameter("Time window (in seconds) for collecting incoming requests before processing begins.")
	private double collectionPeriod = 15.0;

	@Parameter("Maximum number of conflict resolution iterations allowed. " +
		"Each additional iteration may resolve conflicts where multiple partitions attempt to use the same vehicle.")
	private int maxIterations = 2;

	@Parameter("Maximum number of partitions. Each partition handles a subset of requests and vehicles. " +
		"Note: Each partition requires a separate insertion search instance. " +
		"See also: insertionSearchThreadsPerWorker.")
	private int maxPartitions = 4;

	@Parameter("Number of insertion search threads allocated per worker.")
	private int insertionSearchThreadsPerWorker = 4;


	public ParallelInserterParams() {
		super(GROUP_NAME);
	}

	@StringGetter("collectionPeriod")
	public double getCollectionPeriod() {
		return collectionPeriod;
	}

	@StringSetter("collectionPeriod")
	public void setCollectionPeriod(double collectionPeriod) {
		this.collectionPeriod = collectionPeriod;
	}

	@StringGetter("maxIterations")
	public int getMaxIterations() {
		return maxIterations;
	}

	@StringSetter("maxIterations")
	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	@StringGetter("workers")
	public int getMaxPartitions() {
		return maxPartitions;
	}

	@StringSetter("workers")
	public void setMaxPartitions(int maxPartitions) {
		this.maxPartitions = maxPartitions;
	}

	@StringGetter("insertionSearchThreadsPerWorker")
	public int getInsertionSearchThreadsPerWorker() {
		return insertionSearchThreadsPerWorker;
	}

	@StringSetter("insertionSearchThreadsPerWorker")
	public void setInsertionSearchThreadsPerWorker(int insertionSearchThreadsPerWorker) {
		this.insertionSearchThreadsPerWorker = insertionSearchThreadsPerWorker;
	}
}
