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

package org.matsim.contrib.drt.optimizer.insertion.parallel;

import jakarta.validation.constraints.NotNull;
import org.matsim.core.config.ReflectiveConfigGroup;

/**
 * @author Steffen Axer
 */
public class DrtParallelInserterParams extends ReflectiveConfigGroup {

	public DrtParallelInserterParams() {
		super(SET_NAME);
	}

	public enum VehiclesPartitioner {
		ReplicatingVehicleEntryPartitioner, RoundRobinVehicleEntryPartitioner,
		ShiftingRoundRobinVehicleEntryPartitioner
	}

	public enum RequestsPartitioner {RoundRobinRequestsPartitioner, LoadAwareRoundRobinRequestsPartitioner}

	/**
	 * Execution mode for the parallel inserter.
	 */
	public enum ExecutionMode {
		/**
		 * Synchronous batch processing: Collects requests for a period, then blocks mobsim during calculation.
		 * This is the default and well-tested mode.
		 */
		SYNCHRONOUS,

		/**
		 * Asynchronous processing: Calculations run in background while mobsim continues.
		 * Uses DiversionPoint-based validation to ensure calculated insertions are still valid.
		 * EXPERIMENTAL: May provide better performance but requires careful tuning.
		 */
		ASYNCHRONOUS
	}

	public static final String SET_NAME = "parallelInserter";

	@Comment("Time window (in seconds) for collecting incoming requests before processing begins.")
	private double collectionPeriod = 15.0;

	@Comment("Maximum number of conflict resolution iterations allowed. " +
		"Each additional iteration may resolve conflicts where multiple partitions attempt to use the same vehicle.")
	private int maxIterations = 2;

	@Comment("Maximum number of partitions. Each partition handles a subset of requests and vehicles. " +
		"Note: Each partition requires a separate insertion search instance. " +
		"See also: insertionSearchThreadsPerWorker.")
	private int maxPartitions = 4;

	@Comment("Number of insertion search threads allocated per worker.")
	private int insertionSearchThreadsPerWorker = 4;

	@StringGetter("logThreadActivity")
	public boolean isLogThreadActivity() {
		return logThreadActivity;
	}

	@StringSetter("logThreadActivity")
	public void setLogThreadActivity(boolean logThreadActivity) {
		this.logThreadActivity = logThreadActivity;
	}

	@Comment("Enable/Disable thread activity statistics. Note: Disabled by default to improve performance and save memory-")
	private boolean logThreadActivity = false;

	@Comment("Execution mode: SYNCHRONOUS (default, blocks mobsim during calculation) or ASYNCHRONOUS (experimental, calculates in background)")
	@NotNull
	private ExecutionMode executionMode = ExecutionMode.SYNCHRONOUS;

	@Comment("Maximum age in seconds for insertions before the first stop (index 0). " +
		"Only relevant for ASYNCHRONOUS mode. These insertions depend on the exact DiversionPoint " +
		"and may become invalid if the calculation takes too long.")
	private double maxAgeForIndexZeroInsertions = 10.0;

	@Comment("Maximum number of retry attempts when a calculated insertion becomes invalid due to schedule changes. " +
		"Only relevant for ASYNCHRONOUS mode.")
	private int asyncMaxRetries = 3;

	@StringGetter("executionMode")
	public ExecutionMode getExecutionMode() {
		return executionMode;
	}

	@StringSetter("executionMode")
	public void setExecutionMode(ExecutionMode executionMode) {
		this.executionMode = executionMode;
	}

	@StringGetter("maxAgeForIndexZeroInsertions")
	public double getMaxAgeForIndexZeroInsertions() {
		return maxAgeForIndexZeroInsertions;
	}

	@StringSetter("maxAgeForIndexZeroInsertions")
	public void setMaxAgeForIndexZeroInsertions(double maxAgeForIndexZeroInsertions) {
		this.maxAgeForIndexZeroInsertions = maxAgeForIndexZeroInsertions;
	}

	@StringGetter("asyncMaxRetries")
	public int getAsyncMaxRetries() {
		return asyncMaxRetries;
	}

	@StringSetter("asyncMaxRetries")
	public void setAsyncMaxRetries(int asyncMaxRetries) {
		this.asyncMaxRetries = asyncMaxRetries;
	}

	@StringGetter("vehiclesPartitioner")
	public VehiclesPartitioner getVehiclesPartitioner() {
		return vehiclesPartitioner;
	}

	@StringSetter("vehiclesPartitioner")
	public void setVehiclesPartitioner(VehiclesPartitioner vehiclesPartitioner) {
		this.vehiclesPartitioner = vehiclesPartitioner;
	}

	@StringGetter("requestsPartitioner")
	public RequestsPartitioner getRequestsPartitioner() {
		return requestsPartitioner;
	}

	@StringSetter("requestsPartitioner")
	public void setRequestsPartitioner(RequestsPartitioner requestPartitioner) {
		this.requestsPartitioner = requestPartitioner;
	}

	@NotNull
	VehiclesPartitioner vehiclesPartitioner = VehiclesPartitioner.ShiftingRoundRobinVehicleEntryPartitioner;

	@NotNull
	RequestsPartitioner requestsPartitioner = RequestsPartitioner.LoadAwareRoundRobinRequestsPartitioner;

	@StringGetter("collectionPeriod")
	public double getCollectionPeriod() {
		return collectionPeriod;
	}

	@StringSetter("collectionPeriod")
	public void setCollectionPeriod(double collectionPeriod) {
		this.collectionPeriod = collectionPeriod;
	}

	@StringGetter("maxIterations")
	public int getMaxIterations() {
		return maxIterations;
	}

	@StringSetter("maxIterations")
	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	@StringGetter("maxPartitions")
	public int getMaxPartitions() {
		return maxPartitions;
	}

	@StringSetter("maxPartitions")
	public void setMaxPartitions(int maxPartitions) {
		this.maxPartitions = maxPartitions;
	}

	@StringGetter("insertionSearchThreadsPerWorker")
	public int getInsertionSearchThreadsPerWorker() {
		return insertionSearchThreadsPerWorker;
	}

	@StringSetter("insertionSearchThreadsPerWorker")
	public void setInsertionSearchThreadsPerWorker(int insertionSearchThreadsPerWorker) {
		this.insertionSearchThreadsPerWorker = insertionSearchThreadsPerWorker;
	}
}
