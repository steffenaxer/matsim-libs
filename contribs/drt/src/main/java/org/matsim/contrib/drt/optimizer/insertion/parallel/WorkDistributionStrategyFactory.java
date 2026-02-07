/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** */

package org.matsim.contrib.drt.optimizer.insertion.parallel;

import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearch;
import org.matsim.contrib.drt.optimizer.insertion.RequestFleetFilter;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.WorkDistributionMode;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.RequestData;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

/**
 * Factory for creating {@link WorkDistributionStrategy} based on configuration.
 *
 * @author Steffen Axer
 */
public class WorkDistributionStrategyFactory {

	private final DrtParallelInserterParams params;
	private final ForkJoinPool executor;
	private final RequestFleetFilter requestFleetFilter;
	private final Supplier<DrtInsertionSearch> insertionSearchSupplier;

	public WorkDistributionStrategyFactory(
		DrtParallelInserterParams params,
		ForkJoinPool executor,
		RequestFleetFilter requestFleetFilter,
		Supplier<DrtInsertionSearch> insertionSearchSupplier
	) {
		this.params = params;
		this.executor = executor;
		this.requestFleetFilter = requestFleetFilter;
		this.insertionSearchSupplier = insertionSearchSupplier;
	}

	/**
	 * Creates the configured work distribution strategy.
	 */
	public WorkDistributionStrategy create() {
		return switch (params.getWorkDistributionMode()) {
			case PARTITIONING -> {
				List<LocalResultWorker> workers = createLocalWorkers();
				yield createPartitioningStrategy(workers);
			}
			case LOCKING_WORK_STEALING -> {
				List<LockingWorkStealingDistributor.LockingWorker> lockingWorkers = createLockingWorkers();
				LockingWorkStealingDistributor.LockFailureStrategy strategy =
					mapLockFailureStrategy(params.getLockFailureStrategy());
				yield new LockingWorkStealingDistributor(lockingWorkers, executor, strategy, params.getMaxLockRetries());
			}
		};
	}

	private List<LocalResultWorker> createLocalWorkers() {
		List<LocalResultWorker> workers = new ArrayList<>();
		for (int i = 0; i < params.getMaxPartitions(); i++) {
			workers.add(new LocalResultWorker(requestFleetFilter, insertionSearchSupplier.get()));
		}
		return workers;
	}

	private List<LockingWorkStealingDistributor.LockingWorker> createLockingWorkers() {
		List<LockingWorkStealingDistributor.LockingWorker> workers = new ArrayList<>();
		for (int i = 0; i < params.getMaxPartitions(); i++) {
			workers.add(new LockingWorkStealingDistributor.LockingWorker(requestFleetFilter, insertionSearchSupplier.get()));
		}
		return workers;
	}

	private LockingWorkStealingDistributor.LockFailureStrategy mapLockFailureStrategy(
		DrtParallelInserterParams.LockFailureStrategy strategy
	) {
		return switch (strategy) {
			case REQUEUE -> LockingWorkStealingDistributor.LockFailureStrategy.REQUEUE;
			case FIND_ALTERNATIVE -> LockingWorkStealingDistributor.LockFailureStrategy.FIND_ALTERNATIVE;
			case NO_SOLUTION -> LockingWorkStealingDistributor.LockFailureStrategy.NO_SOLUTION;
		};
	}

	private WorkDistributionStrategy createPartitioningStrategy(List<LocalResultWorker> workers) {
		PartitioningWorkDistributor.RequestPartitioner partitioner = createRequestPartitioner();
		return new PartitioningWorkDistributor(workers, executor, partitioner);
	}

	private PartitioningWorkDistributor.RequestPartitioner createRequestPartitioner() {
		return switch (params.getRequestsPartitioner()) {
			case RoundRobinRequestsPartitioner -> this::roundRobinPartition;
			case LoadAwareRoundRobinRequestsPartitioner -> this::loadAwarePartition;
		};
	}

	private List<List<RequestData>> roundRobinPartition(List<RequestData> requests, int numPartitions) {
		List<List<RequestData>> partitions = new ArrayList<>();
		for (int i = 0; i < numPartitions; i++) {
			partitions.add(new ArrayList<>());
		}
		for (int i = 0; i < requests.size(); i++) {
			partitions.get(i % numPartitions).add(requests.get(i));
		}
		return partitions;
	}

	private List<List<RequestData>> loadAwarePartition(List<RequestData> requests, int numPartitions) {
		// Simple load-aware: distribute evenly by count (can be enhanced with complexity estimation)
		List<List<RequestData>> partitions = new ArrayList<>();
		for (int i = 0; i < numPartitions; i++) {
			partitions.add(new ArrayList<>());
		}

		int baseSize = requests.size() / numPartitions;
		int remainder = requests.size() % numPartitions;
		int index = 0;

		for (int i = 0; i < numPartitions; i++) {
			int size = baseSize + (i < remainder ? 1 : 0);
			for (int j = 0; j < size && index < requests.size(); j++) {
				partitions.get(i).add(requests.get(index++));
			}
		}

		return partitions;
	}
}
