package org.matsim.contrib.drt.optimizer.insertion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.passenger.AcceptedDrtRequest;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.drt.scheduler.RequestInsertionScheduler;
import org.matsim.contrib.drt.stops.PassengerStopDurationProvider;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.passenger.PassengerRequestRejectedEvent;
import org.matsim.contrib.dvrp.passenger.PassengerRequestScheduledEvent;
import org.matsim.core.api.experimental.events.EventsManager;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;

import static org.matsim.contrib.drt.optimizer.insertion.DefaultUnplannedRequestInserter.*;

/**
 * @author steffenaxer
 */
public class RequestInsertWorker {
	private static final Logger LOG = LogManager.getLogger(RequestInsertWorker.class);
	private final VehicleEntry.EntryFactory vehicleEntryFactory;
	private final DoubleSupplier timeOfDay;
	private final RequestFleetFilter requestFleetFilter;
	private final DrtInsertionSearch insertionSearch;
	private final BlockingQueue<RequestData> unplannedRequests = new LinkedBlockingQueue<>();;
	private final PassengerStopDurationProvider stopDurationProvider;
	private final DrtOfferAcceptor drtOfferAcceptor;
	private final RequestInsertionScheduler insertionScheduler;
	private final EventsManager eventsManager;
	private final DrtRequestInsertionRetryQueue insertionRetryQueue;
	private final String mode;
	private final Map<Id<DvrpVehicle>, DvrpVehicle> managedVehicles;
	ForkJoinPool forkJoinPool = new ForkJoinPool(4);

	public RequestInsertWorker(VehicleEntry.EntryFactory vehicleEntryFactory,
							   DoubleSupplier timeOfDay,
							   RequestFleetFilter requestFleetFilter,
							   DrtInsertionSearch insertionSearch,
							   PassengerStopDurationProvider stopDurationProvider,
							   DrtOfferAcceptor drtOfferAcceptor,
							   RequestInsertionScheduler insertionScheduler,
							   EventsManager eventsManager,
							   DrtRequestInsertionRetryQueue insertionRetryQueue,
							   String mode,
							   Map<Id<DvrpVehicle>, DvrpVehicle> managedVehicles
	) {
		this.vehicleEntryFactory = vehicleEntryFactory;
		this.timeOfDay = timeOfDay;
		this.requestFleetFilter = requestFleetFilter;
		this.insertionSearch = insertionSearch;
		this.stopDurationProvider = stopDurationProvider;
		this.drtOfferAcceptor = drtOfferAcceptor;
		this.insertionScheduler = insertionScheduler;
		this.eventsManager = eventsManager;
		this.insertionRetryQueue = insertionRetryQueue;
		this.mode = mode;
		this.managedVehicles = managedVehicles;
	}

	public void finish() {
		this.forkJoinPool.shutdown();
	}

	private void scheduleUnplannedRequest(RequestData requestData, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now) {
		DrtRequest req = requestData.getDrtRequest();
		Collection<VehicleEntry> filteredFleet = requestFleetFilter.filter(req, vehicleEntries, now);
		Optional<InsertionWithDetourData> best = insertionSearch.findBestInsertion(req, Collections.unmodifiableCollection(filteredFleet));
		if (best.isEmpty()) {
			requestData.addInsertion(new RequestData.InsertionRecord(null, null, NO_INSERTION_FOUND_CAUSE));
		} else {
			InsertionWithDetourData insertion = best.get();

			double dropoffDuration =
				insertion.detourTimeInfo.dropoffDetourInfo.requestDropoffTime -
					insertion.detourTimeInfo.dropoffDetourInfo.vehicleArrivalTime;

			// accept offered drt ride
			var acceptedRequest = drtOfferAcceptor.acceptDrtOffer(req,
				insertion.detourTimeInfo.pickupDetourInfo.requestPickupTime,
				insertion.detourTimeInfo.dropoffDetourInfo.requestDropoffTime,
				dropoffDuration);

			if (acceptedRequest.isPresent()) {

				requestData.addInsertion(new RequestData.InsertionRecord(insertion, acceptedRequest.get(), INSERTION_ACCEPTED));

				var vehicle = insertion.insertion.vehicleEntry.vehicle;
				VehicleEntry newVehicleEntry = vehicleEntryFactory.create(vehicle, now);
				if (newVehicleEntry != null) {
					vehicleEntries.put(vehicle.getId(), newVehicleEntry);
				} else {
					vehicleEntries.remove(vehicle.getId());
				}

			} else {
				requestData.addInsertion(new RequestData.InsertionRecord(insertion, null, OFFER_REJECTED_CAUSE));
			}
		}
	}


	public void scheduleUnplannedRequests(Collection<RequestData> unplannedRequests) {
		this.unplannedRequests.addAll(unplannedRequests);
	}


	void process(double now) {
		//List<DrtRequest> requestsToRetry = insertionRetryQueue.getRequestsToRetryNow(now);

		var vehicleEntries = forkJoinPool.submit(() -> this.managedVehicles
			.values()
			.parallelStream()
			.map(v -> vehicleEntryFactory.create(v, now))
			.filter(Objects::nonNull)
			.collect(Collectors.toMap(e -> e.vehicle.getId(), e -> e))).join();

		//first retry scheduling old requests
//		for (DrtRequest drtRequest : requestsToRetry) {
//			CompletableFuture<DrtRequest> cf = new CompletableFuture<>();
//			new RequestRecord(drtRequest, cf);
//			scheduleUnplannedRequest(new RequestRecord(drtRequest, cf), vehicleEntries, now);
//		}

		while (!this.unplannedRequests.isEmpty()) {
			RequestData req = unplannedRequests.poll();
			scheduleUnplannedRequest(req, vehicleEntries, now);
		}
	}
}
