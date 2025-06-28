package org.matsim.contrib.drt.optimizer.insertion.parallel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearch;
import org.matsim.contrib.drt.optimizer.insertion.InsertionWithDetourData;
import org.matsim.contrib.drt.optimizer.insertion.RequestFleetFilter;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.drt.scheduler.RequestInsertionScheduler;
import org.matsim.contrib.drt.stops.PassengerStopDurationProvider;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.passenger.PassengerRequestRejectedEvent;
import org.matsim.contrib.dvrp.passenger.PassengerRequestScheduledEvent;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.mobsim.framework.events.MobsimAfterSimStepEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimAfterSimStepListener;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;

import static org.matsim.contrib.drt.optimizer.insertion.DefaultUnplannedRequestInserter.NO_INSERTION_FOUND_CAUSE;
import static org.matsim.contrib.drt.optimizer.insertion.DefaultUnplannedRequestInserter.OFFER_REJECTED_CAUSE;

/**
 * @author steffenaxer
 */
public class RequestInsertWorker implements Runnable, MobsimAfterSimStepListener {
	private static final Logger LOG = LogManager.getLogger(RequestInsertWorker.class);
	private final VehicleEntry.EntryFactory vehicleEntryFactory;
	private final DoubleSupplier timeOfDay;
	private final RequestFleetFilter requestFleetFilter;
	private final DrtInsertionSearch insertionSearch;
	private final Queue<DrtRequest> unplannedRequests = new ConcurrentLinkedQueue<>();
	private final PassengerStopDurationProvider stopDurationProvider;
	private final DrtOfferAcceptor drtOfferAcceptor;
	private final RequestInsertionScheduler insertionScheduler;
	private final EventsManager eventsManager;
	private final DrtRequestInsertionRetryQueue insertionRetryQueue;
	private final String mode;
	private final Map<Id<DvrpVehicle>, DvrpVehicle> managedVehicles;
	private boolean status = true;
	ForkJoinPool forkJoinPool = new ForkJoinPool(4);
	private double lastProcessedTime = -1;

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
		this.status = false;
		this.forkJoinPool.shutdown();
	}

	private void retryOrReject(DrtRequest req, double now, String cause) {
		if (!insertionRetryQueue.tryAddFailedRequest(req, now)) {
			eventsManager.processEvent(new PassengerRequestRejectedEvent(now, mode, req.getId(), req.getPassengerIds(), cause));
			LOG.debug("No insertion found for drt request {} with passenger ids={} fromLinkId={}", req, req.getPassengerIds().stream().map(Object::toString).collect(Collectors.joining(",")), req.getFromLink().getId());
		}
	}

	private void scheduleUnplannedRequest(DrtRequest req, Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries,
										  double now) {
		Collection<VehicleEntry> filteredFleet = requestFleetFilter.filter(req, vehicleEntries, now);
		Optional<InsertionWithDetourData> best = insertionSearch.findBestInsertion(req,
			Collections.unmodifiableCollection(filteredFleet));
		if (best.isEmpty()) {
			retryOrReject(req, now, NO_INSERTION_FOUND_CAUSE);
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
				var vehicle = insertion.insertion.vehicleEntry.vehicle;
				var pickupDropoffTaskPair = insertionScheduler.scheduleRequest(acceptedRequest.get(), insertion);

				VehicleEntry newVehicleEntry = vehicleEntryFactory.create(vehicle, now);
				if (newVehicleEntry != null) {
					vehicleEntries.put(vehicle.getId(), newVehicleEntry);
				} else {
					vehicleEntries.remove(vehicle.getId());
				}

				double expectedPickupTime = pickupDropoffTaskPair.pickupTask.getBeginTime();
				expectedPickupTime = Math.max(expectedPickupTime, acceptedRequest.get().getEarliestStartTime());
				expectedPickupTime += stopDurationProvider.calcPickupDuration(vehicle, req);

				double expectedDropoffTime = pickupDropoffTaskPair.dropoffTask.getBeginTime();
				expectedDropoffTime += stopDurationProvider.calcDropoffDuration(vehicle, req);

				eventsManager.processEvent(
					new PassengerRequestScheduledEvent(now, mode, req.getId(), req.getPassengerIds(), vehicle.getId(),
						expectedPickupTime, expectedDropoffTime));
			} else {
				retryOrReject(req, now, OFFER_REJECTED_CAUSE);
			}
		}
	}

	public void scheduleUnplannedRequests(Collection<DrtRequest> unplannedRequests)
	{
		this.unplannedRequests.addAll(unplannedRequests);
	}

	@Override
	public void run() {
		while (this.status) {
			double now = this.timeOfDay.getAsDouble();

			// Nur einmal pro Sim-Zeitstempel arbeiten
			if (now == lastProcessedTime) {
				try {
					Thread.sleep(50); // Schonende Wartezeit
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					LOG.warn("Worker interrupted", e);
					break;
				}
				continue;
			}
			lastProcessedTime = now;

			List<DrtRequest> requestsToRetry = insertionRetryQueue.getRequestsToRetryNow(now);

			if (unplannedRequests.isEmpty() && requestsToRetry.isEmpty()) {
				continue;
			}

			var vehicleEntries = forkJoinPool.submit(() -> managedVehicles.values()
				.parallelStream()
				.map(v -> vehicleEntryFactory.create(v, now))
				.filter(Objects::nonNull)
				.collect(Collectors.toMap(e -> e.vehicle.getId(), e -> e))).join();

			requestsToRetry.forEach(req -> scheduleUnplannedRequest(req, vehicleEntries, now));

			DrtRequest req;
			while ((req = unplannedRequests.poll()) != null) {
				scheduleUnplannedRequest(req, vehicleEntries, now);
			}
		}
	}

	@Override
	public void notifyMobsimAfterSimStep(MobsimAfterSimStepEvent e) {

	}
}
