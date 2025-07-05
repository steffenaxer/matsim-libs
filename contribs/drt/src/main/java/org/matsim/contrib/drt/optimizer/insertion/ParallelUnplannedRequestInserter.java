/* *********************************************************************** *
 * project: org.matsim.*
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2017 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */

package org.matsim.contrib.drt.optimizer.insertion;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.inject.Provider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Identifiable;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.scheduler.RequestInsertionScheduler;
import org.matsim.contrib.drt.stops.PassengerStopDurationProvider;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.passenger.PassengerRequestRejectedEvent;
import org.matsim.contrib.dvrp.passenger.PassengerRequestScheduledEvent;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.mobsim.framework.MobsimTimer;
import org.matsim.core.mobsim.framework.events.MobsimAfterSimStepEvent;
import org.matsim.core.mobsim.framework.events.MobsimBeforeCleanupEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimAfterSimStepListener;
import org.matsim.core.mobsim.framework.listeners.MobsimBeforeCleanupListener;
import org.matsim.core.mobsim.qsim.InternalInterface;
import org.matsim.core.mobsim.qsim.interfaces.MobsimEngine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;

import static org.matsim.contrib.drt.optimizer.insertion.DefaultUnplannedRequestInserter.NO_INSERTION_FOUND_CAUSE;

/**
 * @author michalm
 */
public class ParallelUnplannedRequestInserter implements UnplannedRequestInserter, MobsimAfterSimStepListener, MobsimEngine, MobsimBeforeCleanupListener {
	private static final Logger LOG = LogManager.getLogger(ParallelUnplannedRequestInserter.class);
	private Double lastProcessingTime;
	private final double collectionPeriod;
	private final String mode;
	private final Fleet fleet;
	private final DoubleSupplier timeOfDay;
	private final EventsManager eventsManager;
	private final Provider<RequestInsertionScheduler> insertionSchedulerProvider;
	private final RequestInsertionScheduler insertionScheduler;
	private final VehicleEntry.EntryFactory vehicleEntryFactory;
	private final Provider<DrtInsertionSearch> insertionSearch;
	private final DrtRequestInsertionRetryQueue insertionRetryQueue;
	private final DrtOfferAcceptor drtOfferAcceptor;
	private final PassengerStopDurationProvider stopDurationProvider;
	private final RequestFleetFilter requestFleetFilter;
	private final List<RequestInsertWorker> workers;
	private final ForkJoinPool inserterExecutorService;
	private final int maxIter;
	private final Map<Id<DvrpVehicle>, SortedSet<RequestData>> solutions = new ConcurrentHashMap<>();
	private final Set<DrtRequest> noSolutions = ConcurrentHashMap.newKeySet();


	public ParallelUnplannedRequestInserter(int threadCount, double collectionPeriod, int maxIter, DrtConfigGroup drtCfg, Fleet fleet, MobsimTimer mobsimTimer,
											EventsManager eventsManager, Provider<RequestInsertionScheduler> insertionSchedulerProvider,
											VehicleEntry.EntryFactory vehicleEntryFactory, Provider<DrtInsertionSearch> insertionSearch,
											DrtRequestInsertionRetryQueue insertionRetryQueue, DrtOfferAcceptor drtOfferAcceptor,
											PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter) {
		this(threadCount, collectionPeriod, maxIter, drtCfg.getMode(), fleet, mobsimTimer::getTimeOfDay, eventsManager, insertionSchedulerProvider, vehicleEntryFactory,
			insertionRetryQueue, insertionSearch, drtOfferAcceptor, stopDurationProvider, requestFleetFilter);
	}

	@VisibleForTesting
	ParallelUnplannedRequestInserter(int threadCount, double collectionPeriod, int maxIter, String mode, Fleet fleet, DoubleSupplier timeOfDay, EventsManager eventsManager,
									 Provider<RequestInsertionScheduler> insertionSchedulerProvider, VehicleEntry.EntryFactory vehicleEntryFactory,
									 DrtRequestInsertionRetryQueue insertionRetryQueue, Provider<DrtInsertionSearch> insertionSearch,
									 DrtOfferAcceptor drtOfferAcceptor, PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter) {
		this.collectionPeriod = collectionPeriod;
		this.mode = mode;
		this.fleet = fleet;
		this.timeOfDay = timeOfDay;
		this.eventsManager = eventsManager;
		this.insertionSchedulerProvider = insertionSchedulerProvider;
		this.insertionScheduler = insertionSchedulerProvider.get();
		this.vehicleEntryFactory = vehicleEntryFactory;
		this.insertionRetryQueue = insertionRetryQueue;
		this.insertionSearch = insertionSearch;
		this.drtOfferAcceptor = drtOfferAcceptor;
		this.stopDurationProvider = stopDurationProvider;
		this.requestFleetFilter = requestFleetFilter;
		this.inserterExecutorService = new ForkJoinPool(threadCount);
		this.workers = getRequestInsertWorker(threadCount);
		this.maxIter = maxIter;
	}

	List<RequestInsertWorker> getRequestInsertWorker(int n) {
		List<RequestInsertWorker> workers = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			RequestInsertWorker requestInsertWorker = new RequestInsertWorker(
				requestFleetFilter,
				insertionSearch.get(),
				drtOfferAcceptor,
				solutions,
				noSolutions);
			workers.add(requestInsertWorker);
		}
		return workers;
	}

	@Override
	public void scheduleUnplannedRequests(Collection<DrtRequest> unplannedRequests) {
		int counter = 0;
		var it = unplannedRequests.iterator();
		while (it.hasNext()) {
			this.workers.get(counter % this.workers.size()).addRequest(new RequestData(it.next()));
			it.remove();
			counter++;
		}

		Verify.verify(unplannedRequests.isEmpty());
	}


	@Override
	public void notifyMobsimAfterSimStep(MobsimAfterSimStepEvent e) {
		double now = timeOfDay.getAsDouble();

		if (this.lastProcessingTime == null) {
			this.lastProcessingTime = now;
		}

		if ((now - lastProcessingTime) >= collectionPeriod) {

			int w = this.workers.stream().mapToInt(RequestInsertWorker::getUnplannedRequestCount).sum();
			LOG.debug("Unplanned requests #{} ", w);

			// Solve requests the first time
			// At this point, we need to generate vehicleEntries for all vehicles
			Map<Id<DvrpVehicle>, VehicleEntry> vehicleEntries = calculateVehicleEntries(now, this.fleet.getVehicles().values());


			lastProcessingTime = now;

			Set<DrtRequest> toBeRejected = new HashSet<>();
			// Retry conflicts
			Integer lastUnsolvedConflicts = null;
			int scheduled = 0;
			for (int i = 0; i < this.maxIter; i++) {
				solve(now, vehicleEntries);
				ConsolidationResult consolidationResult = consolidate();

				// Schedule and clear
				List<RequestData> toBeScheduled = consolidationResult.toBeScheduled;
				toBeScheduled.forEach(r -> schedule(r, now));
				Set<DvrpVehicle> scheduledVehicles = getScheduledVehicles(toBeScheduled);
				this.solutions.clear(); // Clean after having them scheduled!
				scheduled += toBeScheduled.size();

				// Prepare for next round
				toBeRejected.addAll(consolidationResult.toBeRejected);
				this.noSolutions.clear(); // Clean after having them added for next iterations!

				if (toBeRejected.isEmpty()
					|| (lastUnsolvedConflicts != null && toBeRejected.size() == lastUnsolvedConflicts) // not getting better
					|| i == this.maxIter - 1) { // reached iter limit
					LOG.debug("Stopped with rejections #{} ", toBeRejected.size());
					toBeRejected.forEach(s -> reject(s, now, NO_INSERTION_FOUND_CAUSE));
					break;
				}

				// Update vehicle entries for next round
				vehicleEntries = updateVehicleEntries(now, vehicleEntries, scheduledVehicles);
				lastUnsolvedConflicts = toBeRejected.size();
				this.scheduleUnplannedRequests(toBeRejected);
			}
			// Clean workers ultimately
			toBeRejected.forEach(s -> reject(s, now, NO_INSERTION_FOUND_CAUSE));
			LOG.debug("Scheduled requests #{} ", scheduled);

			this.workers.forEach(RequestInsertWorker::clean);
		}
	}

	private static Set<DvrpVehicle> getScheduledVehicles(List<RequestData> toBeScheduled) {
		return toBeScheduled.stream()
			.map(r -> r.getSolution().insertion().get().insertion.vehicleEntry.vehicle)
			.collect(Collectors.toSet());

	}

	private void solve(double now, Map<Id<DvrpVehicle>, VehicleEntry> entries) {
		List<ForkJoinTask<?>> tasks = new ArrayList<>();

		for (RequestInsertWorker worker : this.workers) {
			tasks.add(inserterExecutorService.submit(() -> worker.process(now, entries)));
		}

		tasks.forEach(ForkJoinTask::join);
	}

	private Map<Id<DvrpVehicle>, VehicleEntry> updateVehicleEntries(
		double now,
		Map<Id<DvrpVehicle>, VehicleEntry> currentVehicleEntries,
		Set<DvrpVehicle> toBeUpdated) {

		Set<Id<DvrpVehicle>> toBeDeleted = toBeUpdated.stream()
			.map(Identifiable::getId)
			.collect(Collectors.toSet());

		Map<Id<DvrpVehicle>, VehicleEntry> newlyCreated = calculateVehicleEntries(now, toBeUpdated);


		Map<Id<DvrpVehicle>, VehicleEntry> updated = new HashMap<>();

		currentVehicleEntries.forEach((id, entry) -> {
			if (!toBeDeleted.contains(id)) {
				updated.put(id, entry);
			}
		});

		updated.putAll(newlyCreated);

		return Collections.unmodifiableMap(updated);
	}


	private Map<Id<DvrpVehicle>, VehicleEntry> calculateVehicleEntries(double now, Collection<DvrpVehicle> vehicles) {
		return Collections.unmodifiableMap(
			inserterExecutorService.submit(() ->
				vehicles
					.parallelStream()
					.map(v -> vehicleEntryFactory.create(v, now))
					.filter(Objects::nonNull)
					.collect(Collectors.toMap(
						e -> e.vehicle.getId(), e -> e
					))
			).join()
		);
	}

	record ConsolidationResult(List<RequestData> toBeScheduled, Collection<DrtRequest> toBeRejected) {
	}

	ConsolidationResult consolidate() {

		Set<DrtRequest> allRejection = this.noSolutions;


		ResolvedConflicts resolvedConflicts = resolve(this.solutions);

		// Remaining conflicts, add up into allRejection
		allRejection.addAll(
			resolvedConflicts.conflicts.stream()
				.map(RequestData::getDrtRequest)
				.toList()
		);


		this.workers.forEach(RequestInsertWorker::clean);
		return new ConsolidationResult(resolvedConflicts.noConflicts, allRejection);
	}

	record ResolvedConflicts(List<RequestData> noConflicts, List<RequestData> conflicts) {
	}

	public record WorkerResult(Map<Id<DvrpVehicle>, SortedSet<RequestData>> solutions, Set<DrtRequest> noSolutions) {
	}

	ResolvedConflicts resolve(Map<Id<DvrpVehicle>, SortedSet<RequestData>> data) {
		List<RequestData> noConflicts = new ArrayList<>();
		List<RequestData> conflicts = new ArrayList<>();

		for (var requestDataList : data.values()) {
			if (requestDataList.isEmpty()) continue;

			var iterator = requestDataList.iterator();
			var bestSolution = iterator.next();
			noConflicts.add(bestSolution);

			while (iterator.hasNext()) {
				conflicts.add(iterator.next());
			}
		}

		return new ResolvedConflicts(noConflicts, conflicts);
	}



	void schedule(RequestData requestData, double now) {
		var req = requestData.getDrtRequest();
		var insertion = requestData.getSolution().insertion().get();
		var acceptedRequest = requestData.getSolution().acceptedDrtRequest();
		var vehicle = insertion.insertion.vehicleEntry.vehicle;
		var pickupDropoffTaskPair = insertionScheduler.scheduleRequest(acceptedRequest.get(), insertion);

		double expectedPickupTime = pickupDropoffTaskPair.pickupTask.getBeginTime();
		expectedPickupTime = Math.max(expectedPickupTime, acceptedRequest.get().getEarliestStartTime());
		expectedPickupTime += stopDurationProvider.calcPickupDuration(vehicle, req);

		double expectedDropoffTime = pickupDropoffTaskPair.dropoffTask.getBeginTime();
		expectedDropoffTime += stopDurationProvider.calcDropoffDuration(vehicle, req);

		eventsManager.processEvent(
			new PassengerRequestScheduledEvent(now, mode, req.getId(), req.getPassengerIds(), vehicle.getId(),
				expectedPickupTime, expectedDropoffTime));
		//2System.out.println("Scheduled # "+ requestData.getDrtRequest().getId());

	}

	void reject(DrtRequest req, double now, String text) {
		eventsManager.processEvent(new PassengerRequestRejectedEvent(now, mode, req.getId(), req.getPassengerIds(), text));
	}

	@Override
	public void onPrepareSim() {

	}

	@Override
	public void afterSim() {

	}

	@Override
	public void setInternalInterface(InternalInterface internalInterface) {

	}

	@Override
	public void doSimStep(double time) {

	}

	@Override
	public void notifyMobsimBeforeCleanup(MobsimBeforeCleanupEvent e) {
		inserterExecutorService.shutdown();
		try {
			inserterExecutorService.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
		this.workers.forEach(RequestInsertWorker::finish);
	}


}
