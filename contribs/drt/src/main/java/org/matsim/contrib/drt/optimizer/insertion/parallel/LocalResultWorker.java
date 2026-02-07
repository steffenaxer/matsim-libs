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

/**
 * Worker with LOCAL result collections - no synchronization needed.
 * <p>
 * Uses LinkedHashSet to maintain insertion order for deterministic results.
 *
 * @author Steffen Axer
 */
public class LocalResultWorker {

	private final RequestFleetFilter requestFleetFilter;
	private final DrtInsertionSearch insertionSearch;
	// LinkedHashSet maintains insertion order for determinism
	private final Map<Id<DvrpVehicle>, Set<RequestData>> localSolutions = new LinkedHashMap<>();
	private final Set<DrtRequest> localNoSolutions = new LinkedHashSet<>();

	public LocalResultWorker(RequestFleetFilter requestFleetFilter, DrtInsertionSearch insertionSearch) {
		this.requestFleetFilter = requestFleetFilter;
		this.insertionSearch = insertionSearch;
	}

	public void processRequest(RequestData requestData, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now) {
		DrtRequest req = requestData.getDrtRequest();
		Collection<VehicleEntry> filteredFleet = requestFleetFilter.filter(req, vehicleEntries, now);
		Optional<InsertionWithDetourData> best = insertionSearch.findBestInsertion(req, Collections.unmodifiableCollection(filteredFleet));

		if (best.isEmpty()) {
			localNoSolutions.add(req);
		} else {
			requestData.setSolution(new RequestData.InsertionRecord(best));
			Id<DvrpVehicle> vehicleId = best.get().insertion.vehicleEntry.vehicle.getId();
			localSolutions.computeIfAbsent(vehicleId, k -> new LinkedHashSet<>()).add(requestData);
		}
	}

	public void processAll(Collection<RequestData> requests, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now) {
		for (RequestData request : requests) {
			processRequest(request, vehicleEntries, now);
		}
	}

	public Map<Id<DvrpVehicle>, Set<RequestData>> getLocalSolutions() { return localSolutions; }
	public Set<DrtRequest> getLocalNoSolutions() { return localNoSolutions; }
	public void clean() { localSolutions.clear(); localNoSolutions.clear(); }
}
