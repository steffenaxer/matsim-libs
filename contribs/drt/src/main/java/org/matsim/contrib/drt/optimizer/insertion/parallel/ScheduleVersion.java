package org.matsim.contrib.drt.optimizer.insertion.parallel;

/* *********************************************************************** *
 * project: org.matsim.*
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2025 by the members listed in the COPYING,        *
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

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks schedule versions for vehicles to detect concurrent modifications.
 *
 * <p>Each vehicle's schedule has a version number that is incremented whenever the schedule
 * is modified (e.g., when a new request is scheduled). This allows asynchronous calculations
 * to detect if their computed insertion has become invalid due to a concurrent modification.</p>
 *
 * <p>Thread-safe: Uses atomic operations for version management.</p>
 *
 * @author Steffen Axer
 */
public class ScheduleVersion {

	private final Map<Id<DvrpVehicle>, AtomicLong> versions = new ConcurrentHashMap<>();

	/**
	 * Gets the current version for a vehicle's schedule.
	 *
	 * @param vehicleId The vehicle ID
	 * @return The current version number (starts at 0 for new vehicles)
	 */
	public long getVersion(Id<DvrpVehicle> vehicleId) {
		return versions.computeIfAbsent(vehicleId, k -> new AtomicLong(0)).get();
	}

	/**
	 * Increments and returns the new version for a vehicle's schedule.
	 * Should be called whenever a vehicle's schedule is modified.
	 *
	 * @param vehicleId The vehicle ID
	 * @return The new version number after increment
	 */
	public long incrementVersion(Id<DvrpVehicle> vehicleId) {
		return versions.computeIfAbsent(vehicleId, k -> new AtomicLong(0)).incrementAndGet();
	}

	/**
	 * Checks if a version is still current (no modifications since).
	 *
	 * @param vehicleId The vehicle ID
	 * @param expectedVersion The version to check against
	 * @return true if the current version matches the expected version
	 */
	public boolean isVersionCurrent(Id<DvrpVehicle> vehicleId, long expectedVersion) {
		return getVersion(vehicleId) == expectedVersion;
	}

	/**
	 * Clears all version information. Useful for testing or reset scenarios.
	 */
	public void clear() {
		versions.clear();
	}
}
