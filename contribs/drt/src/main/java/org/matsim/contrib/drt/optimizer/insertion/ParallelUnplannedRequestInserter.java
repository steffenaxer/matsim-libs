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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
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
	private final Provider<RequestInsertionScheduler> insertionScheduler;
	private final VehicleEntry.EntryFactory vehicleEntryFactory;
	private final Provider<DrtInsertionSearch> insertionSearch;
	private final DrtRequestInsertionRetryQueue insertionRetryQueue;
	private final Queue<DrtRequest> requestQueue = new ConcurrentLinkedQueue<>();
	private final DrtOfferAcceptor drtOfferAcceptor;
	private final PassengerStopDurationProvider stopDurationProvider;
	private final RequestFleetFilter requestFleetFilter;
	private final Network network;
	private final List<RequestInsertWorker> workers;
	private final ForkJoinPool inserterExecutorService;
	private final List<Map<Id<DvrpVehicle>, DvrpVehicle>> fleetParts;
	private final Random rnd = MatsimRandom.getLocalInstance();

	public ParallelUnplannedRequestInserter(DrtConfigGroup drtCfg, Fleet fleet, MobsimTimer mobsimTimer,
                                            EventsManager eventsManager, Provider<RequestInsertionScheduler> insertionScheduler,
                                            VehicleEntry.EntryFactory vehicleEntryFactory, Provider<DrtInsertionSearch> insertionSearch,
                                            DrtRequestInsertionRetryQueue insertionRetryQueue, DrtOfferAcceptor drtOfferAcceptor,
											PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter, Network network) {
		this(drtCfg.getMode(), fleet, mobsimTimer::getTimeOfDay, eventsManager, insertionScheduler, vehicleEntryFactory,
				insertionRetryQueue, insertionSearch, drtOfferAcceptor, stopDurationProvider, requestFleetFilter, network);
	}

	@VisibleForTesting
    ParallelUnplannedRequestInserter(String mode, Fleet fleet, DoubleSupplier timeOfDay, EventsManager eventsManager,
                                     Provider<RequestInsertionScheduler> insertionScheduler, VehicleEntry.EntryFactory vehicleEntryFactory,
                                     DrtRequestInsertionRetryQueue insertionRetryQueue, Provider<DrtInsertionSearch> insertionSearch,
                                     DrtOfferAcceptor drtOfferAcceptor, PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter, Network network) {
		this.mode = mode;
		this.fleet = fleet;
		this.timeOfDay = timeOfDay;
		this.eventsManager = eventsManager;
		this.insertionScheduler = insertionScheduler;
		this.vehicleEntryFactory = vehicleEntryFactory;
		this.insertionRetryQueue = insertionRetryQueue;
		this.insertionSearch = insertionSearch;
		this.drtOfferAcceptor = drtOfferAcceptor;
		this.stopDurationProvider = stopDurationProvider;
        this.requestFleetFilter = requestFleetFilter;
		this.network = network;
		this.inserterExecutorService = new ForkJoinPool(3);;
		this.fleetParts = splitFleetIntoParts(this.fleet,3);
		this.workers = getRequestInsertWorker(3);
    }

	List<RequestInsertWorker> getRequestInsertWorker(int n)
	{
		List<RequestInsertWorker> workers = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			RequestInsertWorker requestInsertWorker = new RequestInsertWorker(vehicleEntryFactory,
				timeOfDay,
				requestFleetFilter,
				insertionSearch.get(),
				stopDurationProvider,
				drtOfferAcceptor,
				insertionScheduler.get(),
				eventsManager,
				insertionRetryQueue,
				mode,
				fleetParts.get(i));
			workers.add(requestInsertWorker);
		}
		return workers;
	}

	@Override
	public void scheduleUnplannedRequests(Collection<DrtRequest> unplannedRequests) {
		distributeRoundRobin(unplannedRequests, this.workers);
		unplannedRequests.clear();
	}

	public static void distributeRoundRobin(Collection<DrtRequest> unplannedRequests, List<RequestInsertWorker> workers) {
		if (workers == null || workers.isEmpty()) {
			throw new IllegalArgumentException("Workers could not be empty!");
		}

		List<List<DrtRequest>> partitions = new ArrayList<>();
		for (int i = 0; i < workers.size(); i++) {
			partitions.add(new ArrayList<>());
		}

		Iterator<DrtRequest> iterator = unplannedRequests.iterator();
		int index = 0;
		while (iterator.hasNext()) {
			DrtRequest req = iterator.next();
			partitions.get(index % workers.size()).add(req);
			iterator.remove();
			index++;
		}

		for (int i = 0; i < workers.size(); i++) {
			if (!partitions.get(i).isEmpty()) {
				workers.get(i).scheduleUnplannedRequests(partitions.get(i));
			}
		}
	}

	public static List<Map<Id<DvrpVehicle>, DvrpVehicle>> splitFleetIntoParts(Fleet fleet, int n) {
		if (n <= 0) {
			throw new IllegalArgumentException("Number of fleet parts needs to be larger than 0");
		}

		List<Map<Id<DvrpVehicle>, DvrpVehicle>> parts = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			parts.add(new LinkedHashMap<>());
		}

		int index = 0;
		for (Map.Entry<Id<DvrpVehicle>, DvrpVehicle> entry : fleet.getVehicles().entrySet()) {
			parts.get(index % n).put(entry.getKey(), entry.getValue());
			index++;
		}

		return parts;
	}

	@Override
	public void notifyMobsimAfterSimStep(MobsimAfterSimStepEvent e) {
		double now = timeOfDay.getAsDouble();

		if (this.lastProcessingTime == null) {
			this.lastProcessingTime = now;
		}

		if ((now - lastProcessingTime) >= INTERVAL) {

			List<ForkJoinTask<?>> tasks = new ArrayList<>();

			for (RequestInsertWorker worker : this.workers) {
				tasks.add(inserterExecutorService.submit(() -> worker.process(now)));
			}

			tasks.forEach(ForkJoinTask::join);

			lastProcessingTime = now;
		}
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
