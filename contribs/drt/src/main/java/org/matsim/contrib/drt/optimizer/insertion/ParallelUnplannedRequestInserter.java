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
	private static final Logger log = LogManager.getLogger(ParallelUnplannedRequestInserter.class);
	public static final int INTERVAL = 30;

	private Double lastProcessingTime;
	private final String mode;
	private final Fleet fleet;
	private final DoubleSupplier timeOfDay;
	private final EventsManager eventsManager;
	private final Provider<RequestInsertionScheduler> insertionSchedulerProvider;
	private final RequestInsertionScheduler insertionScheduler;
	private final VehicleEntry.EntryFactory vehicleEntryFactory;
	private final Provider<DrtInsertionSearch> insertionSearch;
	private final DrtRequestInsertionRetryQueue insertionRetryQueue;
	private final List<RequestData> temp = Collections.synchronizedList(new ArrayList<>());
	private final DrtOfferAcceptor drtOfferAcceptor;
	private final PassengerStopDurationProvider stopDurationProvider;
	private final RequestFleetFilter requestFleetFilter;
	private final Network network;
	private final List<RequestInsertWorker> workers;
	private final ForkJoinPool inserterExecutorService;
	private final int maxIter = 3;
	private long counter = 0;

	public ParallelUnplannedRequestInserter(int threadCount, DrtConfigGroup drtCfg, Fleet fleet, MobsimTimer mobsimTimer,
											EventsManager eventsManager, Provider<RequestInsertionScheduler> insertionSchedulerProvider,
											VehicleEntry.EntryFactory vehicleEntryFactory, Provider<DrtInsertionSearch> insertionSearch,
											DrtRequestInsertionRetryQueue insertionRetryQueue, DrtOfferAcceptor drtOfferAcceptor,
											PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter, Network network) {
		this(threadCount, drtCfg.getMode(), fleet, mobsimTimer::getTimeOfDay, eventsManager, insertionSchedulerProvider, vehicleEntryFactory,
			insertionRetryQueue, insertionSearch, drtOfferAcceptor, stopDurationProvider, requestFleetFilter, network);
	}

	@VisibleForTesting
	ParallelUnplannedRequestInserter(int threadCount, String mode, Fleet fleet, DoubleSupplier timeOfDay, EventsManager eventsManager,
									 Provider<RequestInsertionScheduler> insertionSchedulerProvider, VehicleEntry.EntryFactory vehicleEntryFactory,
									 DrtRequestInsertionRetryQueue insertionRetryQueue, Provider<DrtInsertionSearch> insertionSearch,
									 DrtOfferAcceptor drtOfferAcceptor, PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter, Network network) {
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
		this.network = network;
		this.inserterExecutorService = new ForkJoinPool(threadCount);
		this.workers = getRequestInsertWorker(threadCount);
	}

	List<RequestInsertWorker> getRequestInsertWorker(int n) {
		List<RequestInsertWorker> workers = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			RequestInsertWorker requestInsertWorker = new RequestInsertWorker(vehicleEntryFactory,
				timeOfDay,
				requestFleetFilter,
				insertionSearch.get(),
				stopDurationProvider,
				drtOfferAcceptor,
				insertionSchedulerProvider.get(),
				eventsManager,
				mode,
				fleet.getVehicles());
			workers.add(requestInsertWorker);
		}
		return workers;
	}

	@Override
	public void scheduleUnplannedRequests(Collection<DrtRequest> unplannedRequests) {

		var it = unplannedRequests.iterator();
		while (it.hasNext()) {
			this.workers.get((int) (counter % this.workers.size())).addRequest(new RequestData(it.next()));
			it.remove();
			this.counter++;
		}

		Verify.verify(unplannedRequests.isEmpty());
	}


	@Override
	public void notifyMobsimAfterSimStep(MobsimAfterSimStepEvent e) {
		double now = timeOfDay.getAsDouble();

		if (this.lastProcessingTime == null) {
			this.lastProcessingTime = now;
		}

		if ((now - lastProcessingTime) >= INTERVAL) {

			// Solve requests the first time
			// At this point, we need to generate entries for all vehicles
			Map<Id<DvrpVehicle>, VehicleEntry> entries = calculateVehicleEntries(now, this.fleet.getVehicles().values());
			solve(now, entries);

			lastProcessingTime = now;

			Collection<DrtRequest> retryCollection = null;
			// Retry conflicts
			Integer lastUnsolvedConflicts = null;
			for (int i = 0; i < this.maxIter; i++) {
				ConsolidationResult consolidationResult = consolidate(now);
				Collection<DvrpVehicle> schedulesVehicles = consolidationResult.scheduledVehicles;
				retryCollection = consolidationResult.rejectedRequests;

				if (retryCollection.isEmpty() || (lastUnsolvedConflicts != null && retryCollection.size() == lastUnsolvedConflicts)) {
					retryCollection.forEach(s -> reject(s, now, NO_INSERTION_FOUND_CAUSE));
					break;
				}

				var updatedVehicleEntries = calculateVehicleEntries(now, schedulesVehicles);
				lastUnsolvedConflicts = retryCollection.size();
				this.scheduleUnplannedRequests(retryCollection);
				solve(now, updatedVehicleEntries);
				// Clean workers after each iteration
				this.workers.forEach(RequestInsertWorker::clean);
			}
			// Clean workers ultimately
			retryCollection.forEach(s -> reject(s, now, NO_INSERTION_FOUND_CAUSE));

			this.workers.forEach(RequestInsertWorker::clean);
		}
	}

	private void solve(double now, Map<Id<DvrpVehicle>, VehicleEntry> entries) {
		List<ForkJoinTask<?>> tasks = new ArrayList<>();

		for (RequestInsertWorker worker : this.workers) {
			tasks.add(inserterExecutorService.submit(() -> worker.process(now, entries)));
		}

		tasks.forEach(ForkJoinTask::join);
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

	record ConsolidationResult(Set<DvrpVehicle> scheduledVehicles, Collection<DrtRequest> rejectedRequests) {
	}

	ConsolidationResult consolidate(double now) {

		Map<Id<DvrpVehicle>, List<RequestData>> needResolve = new HashMap<>(); // Per worker
		Set<DrtRequest> allRejection = new HashSet<>();

		// Merge results from all workers
		for (RequestInsertWorker worker : this.workers) {
			WorkerResult workerResult = worker.getWorkerResult();

			for (Map.Entry<Id<DvrpVehicle>, List<RequestData>> entry : workerResult.solutions.entrySet()) {
				needResolve.merge(
					entry.getKey(),
					entry.getValue(),
					(existingList, newList) -> {
						existingList.addAll(newList);
						return existingList;
					}
				);
			}

			allRejection.addAll(workerResult.noSolutions);
		}


		ResolvedConflicts resolvedConflicts = resolve(needResolve);

		// Resolved could be submitted
		resolvedConflicts.noConflicts.forEach(s -> schedule(s, now));

		// Remaining conflicts, add up into allRejection
		resolvedConflicts.conflicts.forEach(r -> allRejection.add(r.getDrtRequest()));

		this.workers.forEach(RequestInsertWorker::clean);

		var scheduledVehicle = resolvedConflicts.noConflicts.stream().map(r -> r.getSolution().insertion().get().insertion.vehicleEntry.vehicle).collect(Collectors.toSet());
		return new ConsolidationResult(scheduledVehicle, allRejection);
	}

	record ResolvedConflicts(List<RequestData> noConflicts, List<RequestData> conflicts) {
	}

	record WorkerResult(Map<Id<DvrpVehicle>, List<RequestData>> solutions, List<DrtRequest> noSolutions) {
	}

	//TODO: Prefer solutions with a better score
	ResolvedConflicts resolve(Map<Id<DvrpVehicle>, List<RequestData>> data) {
		List<RequestData> noConflicts = new ArrayList<>();
		List<RequestData> conflicts = new ArrayList<>();

		for (Id<DvrpVehicle> dvrpVehicleId : data.keySet()) {
			var requestData = data.get(dvrpVehicleId);
			requestData.sort(Comparator.comparingDouble(r ->
				r.getSolution().insertion().get().detourTimeInfo.getTotalTimeLoss()
			));

			// Take the best
			var bestSolution = requestData.getFirst();
			noConflicts.add(bestSolution);

			// Reject the rest
			requestData.remove(bestSolution);
			conflicts.addAll(requestData);
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
