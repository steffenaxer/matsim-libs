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
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
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

import com.google.common.annotations.VisibleForTesting;
import org.matsim.core.mobsim.framework.events.MobsimAfterSimStepEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimAfterSimStepListener;
import org.matsim.core.mobsim.qsim.InternalInterface;
import org.matsim.core.mobsim.qsim.interfaces.MobsimEngine;

/**
 * @author michalm
 */
public class DefaultUnplannedRequestInserter implements UnplannedRequestInserter, MobsimAfterSimStepListener, MobsimEngine {
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
	private final RequestInsertWorker worker;

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
		this.worker = new RequestInsertWorker(vehicleEntryFactory,
			timeOfDay,
			requestFleetFilter,
			insertionSearch,
			stopDurationProvider,
			drtOfferAcceptor,
			insertionScheduler,
			eventsManager,
			insertionRetryQueue,
			mode, fleet.getVehicles());
    }

	@Override
	public void scheduleUnplannedRequests(Collection<DrtRequest> unplannedRequests) {
		worker.scheduleUnplannedRequests(unplannedRequests);
		unplannedRequests.clear();
	}

	@Override
	public void notifyMobsimAfterSimStep(MobsimAfterSimStepEvent e) {
		double now = timeOfDay.getAsDouble();

		if(this.lastProcessingTime==null)
		{
			this.lastProcessingTime = timeOfDay.getAsDouble();
		}

		if ((now - lastProcessingTime) >= INTERVAL) {
			worker.process(now);
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
}
