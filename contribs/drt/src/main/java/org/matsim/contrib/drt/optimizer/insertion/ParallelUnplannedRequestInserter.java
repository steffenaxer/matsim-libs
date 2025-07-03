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
import org.matsim.contrib.dvrp.optimizer.Request;
import org.matsim.contrib.dvrp.passenger.PassengerRequestRejectedEvent;
import org.matsim.contrib.dvrp.passenger.PassengerRequestScheduledEvent;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.gbl.MatsimRandom;
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
    public static final int INTERVAL = 60;

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
    private final static List<String> REJECTION_REASONS = List.of("NO_SOLUTION", "VEHICLE_ALREADY_ASSIGNED", "SOLUTION_WITHOUT_ACCEPTANCE");
	private final Set<Id<Request>> scheduled = new HashSet<>();

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
                    insertionRetryQueue,
                    mode,
                    fleet.getVehicles());
            workers.add(requestInsertWorker);
        }
        return workers;
    }

    @Override
    public void scheduleUnplannedRequests(Collection<DrtRequest> unplannedRequests) {
		unplannedRequests.forEach(r ->temp.add(new RequestData((r))));
		unplannedRequests.clear();
    }

	private void assignToWorkers()
	{
		List<List<RequestData>> parts = splitIntoFixedParts(temp, this.workers.size());
		Verify.verify(parts.size() == this.workers.size(), "More parts then workers!");

		for (int i = 0; i < parts.size(); i++) {
			var part = parts.get(i);
			workers.get(i).addRequests(part);
		}
		temp.clear();
	}


    public static <T> List<List<T>> splitIntoFixedParts(List<T> input, int n) {
        List<List<T>> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            result.add(new ArrayList<>());
        }

        for (int i = 0; i < input.size(); i++) {
            result.get(i % n).add(input.get(i));
        }

        return result;
    }


    @Override
    public void notifyMobsimAfterSimStep(MobsimAfterSimStepEvent e) {
        double now = timeOfDay.getAsDouble();

        if (this.lastProcessingTime == null) {
            this.lastProcessingTime = now;
        }

        if ((now - lastProcessingTime) >= INTERVAL) {

            // Solve requests the first time
            solve(now);

            lastProcessingTime = now;

            // Retry conflicts
            Integer lastUnsolvedConflicts = null;
            for (int i = 0; i < this.maxIter; i++) {
                List<DrtRequest> retryList = consolidate(now, i);

                if (retryList.isEmpty() ||  (lastUnsolvedConflicts!= null && retryList.size() == lastUnsolvedConflicts)) {
					retryList.forEach(s -> reject(s, now, NO_INSERTION_FOUND_CAUSE));
                    break;
                }
                lastUnsolvedConflicts = retryList.size();
                this.scheduleUnplannedRequests(retryList);
                solve(now);
            }
        }
    }

    private void solve(double now) {
		assignToWorkers();
		List<ForkJoinTask<?>> tasks = new ArrayList<>();

        for (RequestInsertWorker worker : this.workers) {
            tasks.add(inserterExecutorService.submit(() -> worker.process(now)));
        }

        tasks.forEach(ForkJoinTask::join);
    }

    List<DrtRequest> consolidate(double now, int iteration) {

        List<RequestData> localConflictFree = new ArrayList<>(); // Per worker
        Set<DrtRequest> allRejection = new HashSet<>();

        for (RequestInsertWorker worker : this.workers) {
            Map<String, List<RequestData>> insertions = worker.getCategorizedInsertions();
            List<RequestData> solutions = insertions.getOrDefault("CONFLICT_FREE_SOLUTIONS", Collections.emptyList());
            localConflictFree.addAll(solutions);

            for (String rejectionReason : REJECTION_REASONS) {
                List<RequestData> rejection = worker.getCategorizedInsertions().getOrDefault(rejectionReason, Collections.emptyList());
				rejection.forEach(req -> allRejection.add(req.getDrtRequest()));
            }
            worker.clean();
        }

		for (RequestData requestData : localConflictFree) {
			Verify.verify(requestData.getSolution().acceptedDrtRequest().isPresent(),requestData.getSolution().toString()); //TODO Understand why we have non accepted solutions
		}


        ResolvedConflicts solutions = getConflictFreeInsertions(localConflictFree);
        solutions.noConflicts.forEach(s -> schedule(s, now));

        //Collect all conflicting solutions
		solutions.conflicts.forEach(r -> allRejection.add(r.getDrtRequest()));

		//Remove if already found a global solutions
		solutions.noConflicts.forEach(done -> allRejection.remove(done.getDrtRequest()));

        return new ArrayList<>(allRejection);
    }

    record ResolvedConflicts(List<RequestData> noConflicts, List<RequestData> conflicts) {
    }

    //TODO: Prefer solutions with a better score
    ResolvedConflicts getConflictFreeInsertions(List<RequestData> requestData) {
        Set<DrtRequest> seenRequest = new HashSet<>();
		List<RequestData> noConflicts = new ArrayList<>();
        List<RequestData> conflicts = new ArrayList<>();
        Set<Id<DvrpVehicle>> usedVehicles = new HashSet<>();
        for (RequestData requestDatum : requestData) {

			if(seenRequest.contains(requestDatum.getDrtRequest()))
			{
				continue;
			}
			seenRequest.add(requestDatum.getDrtRequest());
            var vehicleId = requestDatum.getSolution().insertion().get().insertion.vehicleEntry.vehicle.getId();
            if (!usedVehicles.contains(vehicleId)) {
                noConflicts.add(requestDatum);
                usedVehicles.add(vehicleId);
            } else {
                conflicts.add(requestDatum);
            }
        }

        return new ResolvedConflicts(noConflicts, conflicts);
    }

    void schedule(RequestData requestData, double now) {
		Verify.verify(!this.scheduled.contains(requestData.getDrtRequest().getId()),"Request already scheduled! " + requestData.getDrtRequest().getId());
		this.scheduled.add(requestData.getDrtRequest().getId());

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
