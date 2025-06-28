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

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.function.DoubleSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.parallel.RequestInsertWorker;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.scheduler.RequestInsertionScheduler;
import org.matsim.contrib.drt.stops.PassengerStopDurationProvider;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.mobsim.framework.MobsimTimer;

import com.google.common.annotations.VisibleForTesting;
import org.matsim.core.mobsim.framework.events.MobsimAfterSimStepEvent;
import org.matsim.core.mobsim.framework.events.MobsimBeforeCleanupEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimAfterSimStepListener;
import org.matsim.core.mobsim.framework.listeners.MobsimBeforeCleanupListener;
import org.matsim.core.mobsim.qsim.InternalInterface;
import org.matsim.core.mobsim.qsim.interfaces.MobsimEngine;

/**
 * @author michalm
 */
public class DefaultUnplannedRequestInserter implements UnplannedRequestInserter, MobsimAfterSimStepListener, MobsimEngine, MobsimBeforeCleanupListener {
	private static final Logger log = LogManager.getLogger(DefaultUnplannedRequestInserter.class);
	public static final String NO_INSERTION_FOUND_CAUSE = "no_insertion_found";
	public static final String OFFER_REJECTED_CAUSE = "offer_rejected";
	public static final int INTERVAL = 60;

	private Double lastProcessingTime;
	private final String mode;
	private final Fleet fleet;
	private final DoubleSupplier timeOfDay;
	private final EventsManager eventsManager;
	private final RequestInsertionScheduler insertionScheduler;
	private final VehicleEntry.EntryFactory vehicleEntryFactory;
	private final DrtInsertionSearch insertionSearch;
	private final DrtRequestInsertionRetryQueue insertionRetryQueue;
	private final Queue<DrtRequest> requestQueue = new ConcurrentLinkedQueue<>();
	private final DrtOfferAcceptor drtOfferAcceptor;
	private final ForkJoinPool forkJoinPool;
	private final PassengerStopDurationProvider stopDurationProvider;
	private final RequestFleetFilter requestFleetFilter;
	private final static int NUM_THREADS = 2;
	private final Thread[] threads = new Thread[NUM_THREADS];
	private final List<RequestInsertWorker> requestInsertWorkers = new ArrayList<>();

	public DefaultUnplannedRequestInserter(DrtConfigGroup drtCfg, Fleet fleet, MobsimTimer mobsimTimer,
										   EventsManager eventsManager, RequestInsertionScheduler insertionScheduler,
										   VehicleEntry.EntryFactory vehicleEntryFactory, DrtInsertionSearch insertionSearch,
										   DrtRequestInsertionRetryQueue insertionRetryQueue, DrtOfferAcceptor drtOfferAcceptor,
										   ForkJoinPool forkJoinPool, PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter) {
		this(drtCfg.getMode(), fleet, mobsimTimer::getTimeOfDay, eventsManager, insertionScheduler, vehicleEntryFactory,
			insertionRetryQueue, insertionSearch, drtOfferAcceptor, forkJoinPool, stopDurationProvider, requestFleetFilter);
	}

	@VisibleForTesting
	DefaultUnplannedRequestInserter(String mode, Fleet fleet, DoubleSupplier timeOfDay, EventsManager eventsManager,
									RequestInsertionScheduler insertionScheduler, VehicleEntry.EntryFactory vehicleEntryFactory,
									DrtRequestInsertionRetryQueue insertionRetryQueue, DrtInsertionSearch insertionSearch,
									DrtOfferAcceptor drtOfferAcceptor, ForkJoinPool forkJoinPool, PassengerStopDurationProvider stopDurationProvider, RequestFleetFilter requestFleetFilter) {
		this.mode = mode;
		this.fleet = fleet;
		this.timeOfDay = timeOfDay;
		this.eventsManager = eventsManager;
		this.insertionScheduler = insertionScheduler;
		this.vehicleEntryFactory = vehicleEntryFactory;
		this.insertionRetryQueue = insertionRetryQueue;
		this.insertionSearch = insertionSearch;
		this.drtOfferAcceptor = drtOfferAcceptor;
		this.forkJoinPool = forkJoinPool;
		this.stopDurationProvider = stopDurationProvider;
		this.requestFleetFilter = requestFleetFilter;
		this.initializeWorkers();
	}

	public void initializeWorkers() {
		var fleetParts = splitFleetIntoParts(this.fleet, NUM_THREADS);
		for (int i = 0; i < NUM_THREADS; i++) {
			RequestInsertWorker runner = new RequestInsertWorker(vehicleEntryFactory,
				timeOfDay,
				requestFleetFilter,
				insertionSearch,
				stopDurationProvider,
				drtOfferAcceptor,
				insertionScheduler,
				eventsManager,
				insertionRetryQueue,
				mode,
				fleetParts.get(i));
			requestInsertWorkers.add(runner);
			Thread thread = new Thread(runner);
			thread.setDaemon(true);
			thread.setName(RequestInsertWorker.class.toString() + i);
			threads[i] = thread;
			thread.start();
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
	public void scheduleUnplannedRequests(Collection<DrtRequest> unplannedRequests) {
		distributeRoundRobin(unplannedRequests, this.requestInsertWorkers);
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



	@Override
	public void notifyMobsimAfterSimStep(MobsimAfterSimStepEvent e) {

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
		for (int i = 0; i < this.requestInsertWorkers.size(); i++) {
			try {
				requestInsertWorkers.get(i).finish();
				threads[i].join();
			} catch (InterruptedException ex) {
				throw new RuntimeException(ex);
			}
		}
	}
}
