/*
 * *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2023 by the members listed in the COPYING,        *
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
 * *********************************************************************** *
 */

package org.matsim.contrib.zone.skims;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.contrib.dvrp.router.TimeAsTravelDisutility;
import org.matsim.contrib.dvrp.trafficmonitoring.QSimFreeSpeedTravelTime;
import org.matsim.contrib.zone.SquareGridSystem;
import org.matsim.contrib.zone.ZonalSystems;
import org.matsim.contrib.zone.Zone;
import org.matsim.core.router.util.TravelTime;

import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

/**
 * @author Michal Maciejewski, Steffen Axer
 */
public class TimeDependentTravelTimeMatrix implements TravelTimeMatrix {
	public static final Logger LOG = LogManager.getLogger(TimeDependentTravelTimeMatrix.class);
	private static final double TIME_INTERVAL = 3600.;
	private static final double MAX_TIME = 30 * 3600.;
	private final SquareGridSystem gridSystem;
	private final Map<Zone, Node> centralNodes;
	private final TravelTimeMatrices.RoutingParams routingParams;
	private final TimeAsTravelDisutility travelDisutility;
	private final DvrpTravelTimeMatrixParams params;
	private final ForkJoinPool forkJoinPool = new ForkJoinPool(4);
	private Matrix[] travelTimeMatrix;
	private SparseMatrix[] sparseTravelTimeMatrix;

	public TimeDependentTravelTimeMatrix(Network dvrpNetwork, DvrpTravelTimeMatrixParams params, int numberOfThreads, TravelTime travelTime) {
		this.params = params;
		this.gridSystem = new SquareGridSystem(dvrpNetwork.getNodes().values(), params.cellSize);
		this.centralNodes = ZonalSystems.computeMostCentralNodes(dvrpNetwork.getNodes().values(), gridSystem);
		this.travelDisutility = new TimeAsTravelDisutility(travelTime);
		this.routingParams = new TravelTimeMatrices.RoutingParams(dvrpNetwork, travelTime, travelDisutility, numberOfThreads);
		this.initializeMatrix(routingParams, travelDisutility, centralNodes, params);
	}

	public static TimeDependentTravelTimeMatrix createMatrix(Network dvrpNetwork, DvrpTravelTimeMatrixParams params, int numberOfThreads,
															 double qSimTimeStepSize) {
		return new TimeDependentTravelTimeMatrix(dvrpNetwork, params, numberOfThreads, new QSimFreeSpeedTravelTime(qSimTimeStepSize));
	}

	void initializeMatrix(TravelTimeMatrices.RoutingParams routingParams, TimeAsTravelDisutility timeAsTravelDisutility, Map<Zone, Node> centralNodes, DvrpTravelTimeMatrixParams params) {
		int timeBins = (int) (MAX_TIME / TIME_INTERVAL);
		this.travelTimeMatrix = new Matrix[timeBins];
		this.sparseTravelTimeMatrix = new SparseMatrix[timeBins];

		forkJoinPool.submit(() -> IntStream.range(0, timeBins).forEach(i -> {
			LOG.info("Start matrix calculation for departure time {}", i * TIME_INTERVAL);
			long start = System.currentTimeMillis();

			double departureTime = i * TIME_INTERVAL;
			travelTimeMatrix[i] = TravelTimeMatrices.calculateTravelTimeMatrix(routingParams, centralNodes, departureTime);
			sparseTravelTimeMatrix[i] = TravelTimeMatrices.calculateTravelTimeSparseMatrix(routingParams, params.maxNeighborDistance,
					params.maxNeighborTravelTime, departureTime);

			long end = System.currentTimeMillis();
			float calculationTime = (end - start) / 1000F;
			LOG.info("Finished matrix calculation for departure time {}, took {} seconds", i * TIME_INTERVAL, calculationTime);
		})).join();
	}

	@Override
	public int getTravelTime(Node fromNode, Node toNode, double departureTime) {
		int bin = (int) (departureTime / TIME_INTERVAL);
		if (fromNode == toNode) {
			return 0;
		}
		int time = sparseTravelTimeMatrix[bin].get(fromNode, toNode);
		if (time >= 0) {// value is present
			return time;
		}
		return travelTimeMatrix[bin].get(gridSystem.getZone(fromNode), gridSystem.getZone(toNode));
	}

	public int getZonalTravelTime(Node fromNode, Node toNode, double departureTime) {
		int bin = (int) (departureTime / TIME_INTERVAL);
		return travelTimeMatrix[bin].get(gridSystem.getZone(fromNode), gridSystem.getZone(toNode));
	}

	public void update() {
		initializeMatrix(routingParams, travelDisutility, centralNodes, params);
	}
}
