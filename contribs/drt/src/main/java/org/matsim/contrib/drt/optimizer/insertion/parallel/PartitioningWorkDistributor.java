/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** */

package org.matsim.contrib.drt.optimizer.insertion.parallel;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.RequestData;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import static org.matsim.contrib.drt.optimizer.insertion.selective.RequestDataComparators.REQUEST_DATA_COMPARATOR;

/**
 * Work distribution using static partitioning.
 * <p>
 * Requests are divided among workers upfront. Each worker processes its partition independently.
 * Results are merged after all workers complete.
 *
 * @author Steffen Axer
 */
public class PartitioningWorkDistributor implements WorkDistributionStrategy {

	private final List<LocalResultWorker> workers;
	private final ForkJoinPool executor;
	private final RequestPartitioner partitioner;

	public PartitioningWorkDistributor(List<LocalResultWorker> workers, ForkJoinPool executor, RequestPartitioner partitioner) {
		this.workers = workers;
		this.executor = executor;
		this.partitioner = partitioner;
	}

	@Override
	public void distribute(Collection<RequestData> requests, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now) {
		List<List<RequestData>> partitions = partitioner.partition(new ArrayList<>(requests), workers.size());

		List<ForkJoinTask<?>> tasks = new ArrayList<>();
		for (int i = 0; i < workers.size(); i++) {
			final int workerIndex = i;
			final List<RequestData> partition = i < partitions.size() ? partitions.get(i) : Collections.emptyList();

			tasks.add(executor.submit(() -> workers.get(workerIndex).processAll(partition, vehicleEntries, now)));
		}

		// Wait for all workers
		tasks.forEach(ForkJoinTask::join);
	}

	@Override
	public WorkResult collectResults() {
		Map<Id<DvrpVehicle>, SortedSet<RequestData>> mergedSolutions = new TreeMap<>();
		SortedSet<DrtRequest> mergedNoSolutions = new TreeSet<>(Comparator.comparing(r -> r.getId().toString()));

		// Merge in deterministic order (by worker index)
		for (LocalResultWorker worker : workers) {
			// Merge solutions - TreeSet ensures deterministic ordering
			worker.getLocalSolutions().forEach((vehicleId, requests) -> {
				SortedSet<RequestData> set = mergedSolutions.computeIfAbsent(vehicleId,
					k -> new TreeSet<>(REQUEST_DATA_COMPARATOR));
				set.addAll(requests);
			});

			// Merge no-solutions
			mergedNoSolutions.addAll(worker.getLocalNoSolutions());
		}

		return new WorkResult(mergedSolutions, mergedNoSolutions);
	}

	@Override
	public void clean() {
		workers.forEach(LocalResultWorker::clean);
	}

	/**
	 * Partitions requests into N sublists.
	 */
	@FunctionalInterface
	public interface RequestPartitioner {
		List<List<RequestData>> partition(List<RequestData> requests, int numPartitions);
	}
}
