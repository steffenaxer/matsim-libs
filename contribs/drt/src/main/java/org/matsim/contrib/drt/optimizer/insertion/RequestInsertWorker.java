package org.matsim.contrib.drt.optimizer.insertion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.drt.scheduler.RequestInsertionScheduler;
import org.matsim.contrib.drt.stops.PassengerStopDurationProvider;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.passenger.PassengerRequestRejectedEvent;
import org.matsim.core.api.experimental.events.EventsManager;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;

import static org.matsim.contrib.drt.optimizer.insertion.DefaultUnplannedRequestInserter.NO_INSERTION_FOUND_CAUSE;
import static org.matsim.contrib.drt.optimizer.insertion.DefaultUnplannedRequestInserter.OFFER_REJECTED_CAUSE;

/**
 * @author steffenaxer
 */
public class RequestInsertWorker {
    private static final Logger LOG = LogManager.getLogger(RequestInsertWorker.class);
    private static final String OFFER_ACCEPTED = "accepted";
    private final VehicleEntry.EntryFactory vehicleEntryFactory;
    private final RequestFleetFilter requestFleetFilter;
    private final DrtInsertionSearch insertionSearch;
    private final List<RequestData> unplannedRequests = new ArrayList<>();
    private final PassengerStopDurationProvider stopDurationProvider;
    private final DrtOfferAcceptor drtOfferAcceptor;
    private final RequestInsertionScheduler insertionScheduler;
    private final EventsManager eventsManager;
    private final DrtRequestInsertionRetryQueue insertionRetryQueue;
    private final String mode;
    private final Map<Id<DvrpVehicle>, DvrpVehicle> managedVehicles;
    private final ForkJoinPool forkJoinPool = new ForkJoinPool(4);
	private final Map<String, List<RequestData>> categorizedInsertions = new LinkedHashMap<>();
	private final Set<Id<DvrpVehicle>> usedVehicles = new HashSet<>();


	public RequestInsertWorker(VehicleEntry.EntryFactory vehicleEntryFactory,
                               DoubleSupplier timeOfDay,
                               RequestFleetFilter requestFleetFilter,
                               DrtInsertionSearch insertionSearch,
                               PassengerStopDurationProvider stopDurationProvider,
                               DrtOfferAcceptor drtOfferAcceptor,
                               RequestInsertionScheduler insertionScheduler,
                               EventsManager eventsManager,
                               DrtRequestInsertionRetryQueue insertionRetryQueue,
                               String mode, Map<Id<DvrpVehicle>, DvrpVehicle> managedVehicles
    ) {
        this.vehicleEntryFactory = vehicleEntryFactory;
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
        forkJoinPool.shutdown();
        try {
            forkJoinPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void retryOrReject(DrtRequest req, double now, String cause) {
        if (!insertionRetryQueue.tryAddFailedRequest(req, now)) {
            eventsManager.processEvent(new PassengerRequestRejectedEvent(now, mode, req.getId(), req.getPassengerIds(), cause));
            LOG.debug("No insertion found for drt request {} with passenger ids={} fromLinkId={}", req, req.getPassengerIds().stream().map(Object::toString).collect(Collectors.joining(",")), req.getFromLink().getId());
        }
    }

	private void findInsertion(RequestData requestData, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries, double now) {
		DrtRequest req = requestData.getDrtRequest();
		Collection<VehicleEntry> filteredFleet = requestFleetFilter.filter(req, vehicleEntries, now);
		Optional<InsertionWithDetourData> best = insertionSearch.findBestInsertion(req, Collections.unmodifiableCollection(filteredFleet));

		if (best.isEmpty()) {
			requestData.setSolution(new RequestData.InsertionRecord(best, Optional.empty(), NO_INSERTION_FOUND_CAUSE));
			categorizedInsertions
					.computeIfAbsent("NO_SOLUTION", k -> new ArrayList<>())
					.add(requestData);
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
				VehicleEntry newVehicleEntry = vehicleEntryFactory.create(vehicle, now);
				if (newVehicleEntry != null) {
					vehicleEntries.put(vehicle.getId(), newVehicleEntry);
				} else {
					vehicleEntries.remove(vehicle.getId());
				}

				requestData.setSolution(new RequestData.InsertionRecord(best, acceptedRequest, OFFER_ACCEPTED));
				Id<DvrpVehicle> vehicleId = insertion.insertion.vehicleEntry.vehicle.getId();
				if (usedVehicles.contains(vehicleId)) {
					categorizedInsertions
							.computeIfAbsent("VEHICLE_ALREADY_ASSIGNED", k -> new ArrayList<>())
							.add(requestData);
				} else {
					categorizedInsertions
							.computeIfAbsent("CONFLICT_FREE_SOLUTIONS", k -> new ArrayList<>())
							.add(requestData);
					usedVehicles.add(vehicleId);
				}
			} else {
				requestData.setSolution(new RequestData.InsertionRecord(best, Optional.empty(), OFFER_REJECTED_CAUSE));
				categorizedInsertions
						.computeIfAbsent("SOLUTION_WITHOUT_ACCEPTANCE", k -> new ArrayList<>())
						.add(requestData);
			}
		}
	}


	public void addRequests(Collection<RequestData> unplannedRequests) {
        this.unplannedRequests.addAll(unplannedRequests);
    }


    void process(double now) {
        List<DrtRequest> requestsToRetry = insertionRetryQueue.getRequestsToRetryNow(now);

        var vehicleEntries = forkJoinPool.submit(() -> this.managedVehicles
                .values()
                .parallelStream()
                .map(v -> vehicleEntryFactory.create(v, now))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(e -> e.vehicle.getId(), e -> e))).join();

        //first retry scheduling old requests
        //requestsToRetry.forEach(req -> scheduleUnplannedRequest(req, vehicleEntries, now));

		for (RequestData req : unplannedRequests) {
			findInsertion(req, vehicleEntries, now);
		}
    }


	public Map<String, List<RequestData>> getCategorizedInsertions() {
		return categorizedInsertions;
	}


	public void clean() {
		this.usedVehicles.clear();
		this.categorizedInsertions.clear();
		this.unplannedRequests.clear();
	}
}
