package org.matsim.contrib.drt.optimizer.insertion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.drt.scheduler.RequestInsertionScheduler;
import org.matsim.contrib.drt.stops.PassengerStopDurationProvider;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.core.api.experimental.events.EventsManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;

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
	private final ForkJoinPool forkJoinPool = new ForkJoinPool(4);
	private final Map<Id<DvrpVehicle>, List<RequestData>> solutions = new HashMap<>();
	private final List<DrtRequest> noSolutions = new ArrayList<>();

	public RequestInsertWorker(
		RequestFleetFilter requestFleetFilter,
		DrtInsertionSearch insertionSearch,
		DrtOfferAcceptor drtOfferAcceptor
	) {
		this.requestFleetFilter = requestFleetFilter;
		this.insertionSearch = insertionSearch;
		this.drtOfferAcceptor = drtOfferAcceptor;
	}

	public void finish() {
		forkJoinPool.shutdown();
		try {
			forkJoinPool.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}

	public int getUnplannedRequestCount()
	{
		return this.unplannedRequests.size();
	}


	public int getPlannedRequestCount()
	{
		return this.noSolutions.size() + solutions.values().stream().mapToInt(i -> i.size()).sum();
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
				this.solutions.computeIfAbsent(vehicle.getId(), k -> new ArrayList<>()).add(requestData);
			} else {
				this.noSolutions.add(requestData.getDrtRequest());
			}
		}
	}


	public void addRequest(RequestData unplannedRequest) {
		this.unplannedRequests.add(unplannedRequest);
	}


	void process(double now, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries) {

		while (!unplannedRequests.isEmpty()) {
			findInsertion(unplannedRequests.poll(), vehicleEntries, now);
		}

	}


	public ParallelUnplannedRequestInserter.WorkerResult getWorkerResult() {
		return new ParallelUnplannedRequestInserter.WorkerResult(this.solutions, this.noSolutions);
	}


	public void clean() {
		this.solutions.clear();
		this.noSolutions.clear();
		this.unplannedRequests.clear();
	}
}
