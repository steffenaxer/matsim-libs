package org.matsim.contrib.drt.optimizer.insertion.parallel;

import com.google.common.base.Verify;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearch;
import org.matsim.contrib.drt.optimizer.insertion.InsertionWithDetourData;
import org.matsim.contrib.drt.optimizer.insertion.RequestFleetFilter;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.RequestData;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.matsim.contrib.drt.optimizer.insertion.selective.RequestDataComparators.REQUEST_DATA_COMPARATOR;

/**
 * @author steffenaxer
 */
public class RequestInsertWorker {
	private static final Logger LOG = LogManager.getLogger(RequestInsertWorker.class);
	private static final String OFFER_ACCEPTED = "accepted";
	private final RequestFleetFilter requestFleetFilter;
	private final DrtInsertionSearch insertionSearch;
	private final Queue<RequestData> unplannedRequests = new ConcurrentLinkedQueue<>();
	private final DrtOfferAcceptor drtOfferAcceptor;
	private final Map<Id<DvrpVehicle>, SortedSet<RequestData>> solutions;
	private final SortedSet<DrtRequest> noSolutions;

	public RequestInsertWorker(
		RequestFleetFilter requestFleetFilter,
		DrtInsertionSearch insertionSearch,
		DrtOfferAcceptor drtOfferAcceptor,
		Map<Id<DvrpVehicle>, SortedSet<RequestData>> solutions, SortedSet<DrtRequest> noSolutions) {
		this.requestFleetFilter = requestFleetFilter;
		this.insertionSearch = insertionSearch;
		this.drtOfferAcceptor = drtOfferAcceptor;
		this.solutions = solutions;
		this.noSolutions = noSolutions;
	}

	public int getUnplannedRequestCount()
	{
		return this.unplannedRequests.size();
	}


	public int getPlannedRequestCount()
	{
		return this.noSolutions.size() + solutions.values().stream().mapToInt(Set::size).sum();
	}

	private static SortedSet<RequestData> createTreeSet()
	{
		return new ConcurrentSkipListSet<>(new TreeSet<>(REQUEST_DATA_COMPARATOR));
	}

	private void findInsertion(RequestData requestData, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now) {
		DrtRequest req = requestData.getDrtRequest();
		Collection<VehicleEntry> filteredFleet = requestFleetFilter.filter(req, vehicleEntries, now);
		Optional<InsertionWithDetourData> best = insertionSearch.findBestInsertion(req, Collections.unmodifiableCollection(filteredFleet));

		if (best.isEmpty()) {
			this.noSolutions.add(requestData.getDrtRequest());
		} else {
			InsertionWithDetourData insertion = best.get();
			double dropoffDuration = insertion.detourTimeInfo.dropoffDetourInfo.requestDropoffTime -
				insertion.detourTimeInfo.dropoffDetourInfo.vehicleArrivalTime;

			var acceptedRequest = drtOfferAcceptor.acceptDrtOffer(req,
				insertion.detourTimeInfo.pickupDetourInfo.requestPickupTime,
				insertion.detourTimeInfo.dropoffDetourInfo.requestDropoffTime,
				dropoffDuration);

			if (acceptedRequest.isPresent()) {
				var vehicle = insertion.insertion.vehicleEntry.vehicle;
				requestData.setSolution(new RequestData.InsertionRecord(best, acceptedRequest, OFFER_ACCEPTED));
				this.solutions.computeIfAbsent(vehicle.getId(), k -> createTreeSet()).add(requestData);
			} else {
				this.noSolutions.add(requestData.getDrtRequest());
			}
		}
	}


	void process(double now, Collection<RequestData> requestDataPartition, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries) {
		this.unplannedRequests.addAll(requestDataPartition);

		if (!requestDataPartition.isEmpty()) {
			Verify.verify(!vehicleEntries.isEmpty(), "Requests have been assigned to a worker without vehicleEntries.");
		}

		while (!unplannedRequests.isEmpty()) {
			findInsertion(unplannedRequests.poll(), vehicleEntries, now);
		}

	}


	public void clean() {
		this.unplannedRequests.clear();
	}
}
