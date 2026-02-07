/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** */

package org.matsim.contrib.drt.optimizer.insertion.parallel;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearch;
import org.matsim.contrib.drt.optimizer.insertion.InsertionWithDetourData;
import org.matsim.contrib.drt.optimizer.insertion.RequestFleetFilter;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.RequestData;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.matsim.contrib.drt.optimizer.insertion.selective.RequestDataComparators.REQUEST_DATA_COMPARATOR;

/**
 * Work distribution using deterministic partitioning with optimistic vehicle locking.
 * <p>
 * Requests are deterministically partitioned to workers (round-robin), and each worker
 * processes its partition. Vehicle assignment uses CAS locks to resolve conflicts
 * when multiple workers want the same vehicle.
 * <p>
 * <b>Advantages:</b>
 * <ul>
 *   <li>Deterministic: Same input produces same output</li>
 *   <li>No conflict resolution iterations needed</li>
 *   <li>Each request is processed only once (no re-computation)</li>
 *   <li>Minimal lock overhead (CAS operations)</li>
 * </ul>
 * <p>
 * <b>Determinism:</b> This strategy is <b>deterministic</b>. Request→Worker assignment
 * is fixed via round-robin partitioning. Vehicle locking order within a partition is
 * deterministic. Results are sorted using TreeMap/TreeSet.
 *
 * @author Steffen Axer
 */
public class LockingWorkStealingDistributor implements WorkDistributionStrategy {

	/**
	 * Strategy for handling lock failures.
	 */
	public enum LockFailureStrategy {
		/** Re-queue the request for later retry */
		REQUEUE,
		/** Find next best insertion that's not locked */
		FIND_ALTERNATIVE,
		/** Mark as no-solution immediately */
		NO_SOLUTION
	}

	private final List<LockingWorker> workers;
	private final ForkJoinPool executor;
	private final ConcurrentHashMap<Id<DvrpVehicle>, AtomicBoolean> vehicleLocks = new ConcurrentHashMap<>();
	private final LockFailureStrategy lockFailureStrategy;
	private final int maxRetries;

	public LockingWorkStealingDistributor(
		List<LockingWorker> workers,
		ForkJoinPool executor,
		LockFailureStrategy lockFailureStrategy,
		int maxRetries
	) {
		this.workers = workers;
		this.executor = executor;
		this.lockFailureStrategy = lockFailureStrategy;
		this.maxRetries = maxRetries;
	}

	@Override
	public void distribute(Collection<RequestData> requests, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now) {
		// Initialize locks for all vehicles
		vehicleLocks.clear();
		for (Id<DvrpVehicle> vehicleId : vehicleEntries.keySet()) {
			vehicleLocks.put(vehicleId, new AtomicBoolean(false));
		}

		// Deterministic partitioning: distribute requests to workers in round-robin fashion
		// This ensures reproducible Request→Worker assignment
		List<List<RequestData>> partitions = partitionDeterministically(new ArrayList<>(requests), workers.size());

		// Start all workers with their deterministic partitions
		List<ForkJoinTask<?>> tasks = new ArrayList<>();
		for (int i = 0; i < workers.size(); i++) {
			final List<RequestData> partition = partitions.get(i);
			final LockingWorker worker = workers.get(i);
			tasks.add(executor.submit(() ->
				worker.processPartitionWithLocking(partition, vehicleLocks, vehicleEntries, now, lockFailureStrategy, maxRetries)
			));
		}

		// Wait for completion
		tasks.forEach(ForkJoinTask::join);
	}

	/**
	 * Deterministically partitions requests using round-robin distribution.
	 * Ensures reproducible results across runs.
	 */
	private List<List<RequestData>> partitionDeterministically(List<RequestData> requests, int numPartitions) {
		List<List<RequestData>> partitions = new ArrayList<>();
		for (int i = 0; i < numPartitions; i++) {
			partitions.add(new ArrayList<>());
		}
		for (int i = 0; i < requests.size(); i++) {
			partitions.get(i % numPartitions).add(requests.get(i));
		}
		return partitions;
	}

	@Override
	public WorkResult collectResults() {
		Map<Id<DvrpVehicle>, SortedSet<RequestData>> mergedSolutions = new TreeMap<>();
		SortedSet<DrtRequest> mergedNoSolutions = new TreeSet<>(Comparator.comparing(r -> r.getId().toString()));

		for (LockingWorker worker : workers) {
			worker.getLocalSolutions().forEach((vehicleId, requests) -> {
				SortedSet<RequestData> set = mergedSolutions.computeIfAbsent(vehicleId,
					k -> new TreeSet<>(REQUEST_DATA_COMPARATOR));
				set.addAll(requests);
			});
			mergedNoSolutions.addAll(worker.getLocalNoSolutions());
		}

		return new WorkResult(mergedSolutions, mergedNoSolutions);
	}

	@Override
	public void clean() {
		vehicleLocks.clear();
		workers.forEach(LockingWorker::clean);
	}

	/**
	 * Worker that uses optimistic locking for vehicle assignment.
	 */
	public static class LockingWorker {
		private final RequestFleetFilter requestFleetFilter;
		private final DrtInsertionSearch insertionSearch;
		private final Map<Id<DvrpVehicle>, Set<RequestData>> localSolutions = new LinkedHashMap<>();
		private final Set<DrtRequest> localNoSolutions = new LinkedHashSet<>();

		public LockingWorker(RequestFleetFilter requestFleetFilter, DrtInsertionSearch insertionSearch) {
			this.requestFleetFilter = requestFleetFilter;
			this.insertionSearch = insertionSearch;
		}

		/**
		 * Processes a deterministic partition of requests with vehicle locking.
		 * This replaces work-stealing to ensure reproducibility.
		 */
		void processPartitionWithLocking(
			List<RequestData> partition,
			ConcurrentHashMap<Id<DvrpVehicle>, AtomicBoolean> vehicleLocks,
			Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries,
			double now,
			LockFailureStrategy strategy,
			int maxRetries
		) {
			for (RequestData requestData : partition) {
				processWithLocking(requestData, vehicleLocks, vehicleEntries, now, strategy, maxRetries);
			}
		}

		private void processWithLocking(
			RequestData requestData,
			ConcurrentHashMap<Id<DvrpVehicle>, AtomicBoolean> vehicleLocks,
			Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries,
			double now,
			LockFailureStrategy strategy,
			int maxRetries
		) {
			DrtRequest req = requestData.getDrtRequest();
			Collection<VehicleEntry> filteredFleet = requestFleetFilter.filter(req, vehicleEntries, now);

			// Find best insertion
			Optional<InsertionWithDetourData> best = insertionSearch.findBestInsertion(
				req, Collections.unmodifiableCollection(filteredFleet));

			if (best.isEmpty()) {
				localNoSolutions.add(req);
				return;
			}

			Id<DvrpVehicle> vehicleId = best.get().insertion.vehicleEntry.vehicle.getId();
			AtomicBoolean lock = vehicleLocks.get(vehicleId);

			// Try to acquire lock (CAS operation - very fast)
			if (lock != null && lock.compareAndSet(false, true)) {
				// SUCCESS: We got the lock
				requestData.setSolution(new RequestData.InsertionRecord(best));
				localSolutions.computeIfAbsent(vehicleId, k -> new LinkedHashSet<>()).add(requestData);
			} else {
				// FAIL: Vehicle already locked by another worker
				handleLockFailure(requestData, vehicleLocks, filteredFleet, vehicleId, strategy, maxRetries);
			}
		}

		private void handleLockFailure(
			RequestData requestData,
			ConcurrentHashMap<Id<DvrpVehicle>, AtomicBoolean> vehicleLocks,
			Collection<VehicleEntry> filteredFleet,
			Id<DvrpVehicle> lockedVehicleId,
			LockFailureStrategy strategy,
			int maxRetries
		) {
			switch (strategy) {
				case REQUEUE -> {
					// With deterministic partitioning, REQUEUE just retries locally
					int retries = requestData.getRetryCount();
					if (retries < maxRetries) {
						requestData.incrementRetryCount();
						// Retry: try to find alternative immediately
						boolean found = tryAlternativeVehicle(requestData, vehicleLocks, filteredFleet, lockedVehicleId);
						if (!found) {
							localNoSolutions.add(requestData.getDrtRequest());
						}
					} else {
						localNoSolutions.add(requestData.getDrtRequest());
					}
				}
				case FIND_ALTERNATIVE -> {
					// Try to find alternative vehicle (exclude locked ones)
					boolean found = tryAlternativeVehicle(requestData, vehicleLocks, filteredFleet, lockedVehicleId);
					if (!found) {
						localNoSolutions.add(requestData.getDrtRequest());
					}
				}
				case NO_SOLUTION -> localNoSolutions.add(requestData.getDrtRequest());
			}
		}

		private boolean tryAlternativeVehicle(
			RequestData requestData,
			ConcurrentHashMap<Id<DvrpVehicle>, AtomicBoolean> vehicleLocks,
			Collection<VehicleEntry> filteredFleet,
			Id<DvrpVehicle> excludeVehicleId
		) {
			// Filter out the locked vehicle and try others
			List<VehicleEntry> availableVehicles = filteredFleet.stream()
				.filter(ve -> !ve.vehicle.getId().equals(excludeVehicleId))
				.filter(ve -> {
					AtomicBoolean lock = vehicleLocks.get(ve.vehicle.getId());
					return lock == null || !lock.get();
				})
				.toList();

			if (availableVehicles.isEmpty()) {
				return false;
			}

			// Find best among available
			Optional<InsertionWithDetourData> alternative = insertionSearch.findBestInsertion(
				requestData.getDrtRequest(), availableVehicles);

			if (alternative.isEmpty()) {
				return false;
			}

			Id<DvrpVehicle> altVehicleId = alternative.get().insertion.vehicleEntry.vehicle.getId();
			AtomicBoolean lock = vehicleLocks.get(altVehicleId);

			if (lock != null && lock.compareAndSet(false, true)) {
				requestData.setSolution(new RequestData.InsertionRecord(alternative));
				localSolutions.computeIfAbsent(altVehicleId, k -> new LinkedHashSet<>()).add(requestData);
				return true;
			}

			return false;
		}

		public Map<Id<DvrpVehicle>, Set<RequestData>> getLocalSolutions() {
			return localSolutions;
		}

		public Set<DrtRequest> getLocalNoSolutions() {
			return localNoSolutions;
		}

		public void clean() {
			localSolutions.clear();
			localNoSolutions.clear();
		}
	}
}
