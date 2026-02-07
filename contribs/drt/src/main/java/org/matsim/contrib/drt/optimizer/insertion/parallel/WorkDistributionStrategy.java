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

/**
 * Strategy interface for distributing work to parallel workers.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li>{@link PartitioningWorkDistributor} - Static partitioning of requests</li>
 *   <li>{@link WorkStealingDistributor} - Dynamic work-stealing from central queue</li>
 * </ul>
 * <p>
 * <b>Determinism Guarantee:</b> Both modes produce deterministic results.
 * While processing order may vary (especially with work-stealing), the final
 * results are sorted using {@code REQUEST_DATA_COMPARATOR} and {@code TreeMap/TreeSet},
 * ensuring identical output for identical input regardless of thread scheduling.
 *
 * @author Steffen Axer
 */
public interface WorkDistributionStrategy {

	/**
	 * Distributes work to workers and executes parallel processing.
	 *
	 * @param requests       Requests to process
	 * @param vehicleEntries Available vehicles
	 * @param now            Current simulation time
	 */
	void distribute(Collection<RequestData> requests, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now);

	/**
	 * Collects and merges results from all workers.
	 *
	 * @return Merged results (solutions and no-solutions)
	 */
	WorkResult collectResults();

	/**
	 * Cleans up worker state for next round.
	 */
	void clean();

	/**
	 * Result container for merged worker results.
	 */
	record WorkResult(
		Map<Id<DvrpVehicle>, SortedSet<RequestData>> solutions,
		SortedSet<DrtRequest> noSolutions
	) {
		public static WorkResult empty() {
			return new WorkResult(new TreeMap<>(), new TreeSet<>(Comparator.comparing(r -> r.getId().toString())));
		}
	}

	/**
	 * Distribution mode.
	 */
	enum Mode {
		/**
		 * Static partitioning - requests are divided upfront among workers.
		 * Lower overhead, but may have unequal load distribution.
		 */
		PARTITIONING,

		/**
		 * Dynamic work-stealing - workers pull from central queue.
		 * Better load balancing, but slightly higher overhead.
		 */
		WORK_STEALING
	}
}
