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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.InsertionWithDetourData;
import org.matsim.contrib.drt.passenger.DrtRequest;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages asynchronous insertion calculations with DiversionPoint-based validation.
 *
 * <p>This class implements the asynchronous insertion strategy where calculations
 * run in background threads while the mobsim continues. Calculated insertions are
 * validated before scheduling to ensure they are still applicable.</p>
 *
 * <h2>Key Concepts:</h2>
 * <ul>
 *   <li><b>DiversionPoint-based calculation:</b> Uses the vehicle's diversion point
 *       as the basis for calculations, making them stable regardless of vehicle movement</li>
 *   <li><b>Schedule versioning:</b> Detects concurrent modifications to vehicle schedules</li>
 *   <li><b>Lightweight validation:</b> Only checks structure changes, not SlackTime
 *       (since DiversionPoint-based calculation handles time progression)</li>
 * </ul>
 *
 * @author Steffen Axer
 * @see DiversionPointVehicleEntry
 * @see InsertionValidator
 */
public class AsyncInsertionStrategy {

	private static final Logger LOG = LogManager.getLogger(AsyncInsertionStrategy.class);

	private final ScheduleVersion scheduleVersion;
	private final InsertionValidator insertionValidator;
	private final int maxRetries;

	// Metrics
	private final AtomicLong totalCalculations = new AtomicLong(0);
	private final AtomicLong validInsertions = new AtomicLong(0);
	private final AtomicLong invalidInsertions = new AtomicLong(0);
	private final AtomicLong retriedInsertions = new AtomicLong(0);

	// Queue for solutions ready to be validated and scheduled
	private final Queue<InsertionCalculationResult> solutionQueue = new ConcurrentLinkedQueue<>();

	public AsyncInsertionStrategy(double maxAgeForIndexZeroInsertions, int maxRetries) {
		this.scheduleVersion = new ScheduleVersion();
		this.insertionValidator = new InsertionValidator(maxAgeForIndexZeroInsertions);
		this.maxRetries = maxRetries;
	}

	/**
	 * Creates a DiversionPoint-based VehicleEntry for async calculation.
	 * The entry's SlackTimes are adjusted based on the time until the diversion point.
	 *
	 * @param baseEntry The original VehicleEntry
	 * @return A DiversionPointVehicleEntry suitable for async calculation
	 */
	public DiversionPointVehicleEntry createAsyncVehicleEntry(VehicleEntry baseEntry) {
		// The DiversionPoint is already captured in the baseEntry's start position
		// for DRIVE tasks. We just need to wrap it with version tracking.
		var diversionPoint = new org.matsim.contrib.dvrp.util.LinkTimePair(
			baseEntry.start.link,
			baseEntry.start.time
		);
		return new DiversionPointVehicleEntry(baseEntry, diversionPoint);
	}

	/**
	 * Records that a calculation has started for a vehicle.
	 *
	 * @param vehicleId The vehicle being calculated for
	 * @return The schedule version at the start of calculation
	 */
	public long recordCalculationStart(Id<DvrpVehicle> vehicleId) {
		totalCalculations.incrementAndGet();
		return scheduleVersion.getVersion(vehicleId);
	}

	/**
	 * Records a completed calculation result.
	 *
	 * @param result The calculation result to queue for validation
	 */
	public void recordCalculationResult(InsertionCalculationResult result) {
		solutionQueue.offer(result);
	}

	/**
	 * Validates and returns the next ready insertion, if any.
	 *
	 * @param currentVehicleEntries Current vehicle entries for validation
	 * @param currentTime Current simulation time
	 * @return Optional containing the validated result, or empty if none available or valid
	 */
	public Optional<ValidatedInsertionResult> pollAndValidate(
			Map<Id<DvrpVehicle>, VehicleEntry> currentVehicleEntries,
			double currentTime) {

		InsertionCalculationResult result = solutionQueue.poll();
		if (result == null) {
			return Optional.empty();
		}

		return validateResult(result, currentVehicleEntries, currentTime);
	}

	/**
	 * Validates a calculation result against the current state.
	 */
	private Optional<ValidatedInsertionResult> validateResult(
			InsertionCalculationResult result,
			Map<Id<DvrpVehicle>, VehicleEntry> currentVehicleEntries,
			double currentTime) {

		Id<DvrpVehicle> vehicleId = result.vehicleId();

		// 1. Check schedule version (fast path)
		if (!scheduleVersion.isVersionCurrent(vehicleId, result.scheduleVersionAtCalculation())) {
			LOG.debug("Schedule version mismatch for vehicle {} - insertion invalidated", vehicleId);
			invalidInsertions.incrementAndGet();
			return Optional.of(new ValidatedInsertionResult(result, ValidationStatus.INVALID_VERSION, null));
		}

		// 2. Get current vehicle entry for detailed validation
		VehicleEntry currentEntry = currentVehicleEntries.get(vehicleId);
		if (currentEntry == null) {
			LOG.debug("Vehicle {} no longer available", vehicleId);
			invalidInsertions.incrementAndGet();
			return Optional.of(new ValidatedInsertionResult(result, ValidationStatus.VEHICLE_UNAVAILABLE, null));
		}

		// 3. Detailed validation using InsertionValidator
		InsertionValidator.ValidationResult validationResult = insertionValidator.validate(
			result.insertion(),
			result.originalEntry(),
			currentEntry,
			result.calculationTime(),
			currentTime
		);

		switch (validationResult) {
			case VALID:
				validInsertions.incrementAndGet();
				return Optional.of(new ValidatedInsertionResult(result, ValidationStatus.VALID, currentEntry));

			case NEEDS_SLACK_RECHECK:
				// For index 0 insertions that are too old, we need to revalidate slack
				if (isSlackStillSufficient(result, currentEntry)) {
					validInsertions.incrementAndGet();
					return Optional.of(new ValidatedInsertionResult(result, ValidationStatus.VALID, currentEntry));
				} else {
					invalidInsertions.incrementAndGet();
					return Optional.of(new ValidatedInsertionResult(result, ValidationStatus.INVALID_SLACK, null));
				}

			case INVALID_STRUCTURE:
			case INVALID_CAPACITY:
			default:
				invalidInsertions.incrementAndGet();
				return Optional.of(new ValidatedInsertionResult(result, ValidationStatus.INVALID_STRUCTURE, null));
		}
	}

	/**
	 * Quick slack time recheck for index 0 insertions.
	 */
	private boolean isSlackStillSufficient(InsertionCalculationResult result, VehicleEntry currentEntry) {
		int pickupIdx = result.insertion().insertion.pickup.index;
		int dropoffIdx = result.insertion().insertion.dropoff.index;

		double pickupTimeLoss = result.insertion().detourTimeInfo.pickupDetourInfo.pickupTimeLoss;
		double totalTimeLoss = result.insertion().detourTimeInfo.getTotalTimeLoss();

		double currentPickupSlack = currentEntry.getSlackTime(pickupIdx);
		double currentDropoffSlack = currentEntry.getSlackTime(dropoffIdx);

		return currentPickupSlack >= pickupTimeLoss && currentDropoffSlack >= totalTimeLoss;
	}

	/**
	 * Called when a request is successfully scheduled.
	 * Increments the schedule version for the vehicle.
	 *
	 * @param vehicleId The vehicle whose schedule was modified
	 */
	public void onRequestScheduled(Id<DvrpVehicle> vehicleId) {
		scheduleVersion.incrementVersion(vehicleId);
	}

	/**
	 * Gets the maximum number of retries for failed validations.
	 */
	public int getMaxRetries() {
		return maxRetries;
	}

	/**
	 * Returns the current validation success rate.
	 */
	public double getValidationSuccessRate() {
		long total = validInsertions.get() + invalidInsertions.get();
		return total > 0 ? (double) validInsertions.get() / total : 1.0;
	}

	/**
	 * Returns metrics as a formatted string.
	 */
	public String getMetricsSummary() {
		return String.format(
			"AsyncInsertionStrategy: total=%d, valid=%d, invalid=%d, retried=%d, successRate=%.2f%%",
			totalCalculations.get(),
			validInsertions.get(),
			invalidInsertions.get(),
			retriedInsertions.get(),
			getValidationSuccessRate() * 100
		);
	}

	/**
	 * Clears all queued solutions. Useful for cleanup.
	 */
	public void clearQueue() {
		solutionQueue.clear();
	}

	/**
	 * Returns whether there are pending solutions to process.
	 */
	public boolean hasPendingSolutions() {
		return !solutionQueue.isEmpty();
	}

	/**
	 * Result of an insertion calculation, including metadata for validation.
	 */
	public record InsertionCalculationResult(
		DrtRequest request,
		InsertionWithDetourData insertion,
		Id<DvrpVehicle> vehicleId,
		VehicleEntry originalEntry,
		long scheduleVersionAtCalculation,
		double calculationTime
	) {}

	/**
	 * Status of validation.
	 */
	public enum ValidationStatus {
		VALID,
		INVALID_VERSION,
		INVALID_STRUCTURE,
		INVALID_SLACK,
		VEHICLE_UNAVAILABLE
	}

	/**
	 * Result of validation including the status and current entry (if valid).
	 */
	public record ValidatedInsertionResult(
		InsertionCalculationResult calculationResult,
		ValidationStatus status,
		VehicleEntry currentEntry
	) {
		public boolean isValid() {
			return status == ValidationStatus.VALID;
		}
	}
}
