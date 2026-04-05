/*
 * *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2026 by the members listed in the COPYING,        *
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

package org.matsim.contrib.drt.optimizer.insertion.extensive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.NetworkFactory;
import org.matsim.api.core.v01.network.Node;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.dvrp.path.OneToManyPathSearch;
import org.matsim.contrib.dvrp.router.TimeAsTravelDisutility;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.speedy.SpeedyCHGraph;
import org.matsim.core.router.speedy.InertialFlowCutter;
import org.matsim.core.router.speedy.SpeedyCHBuilder;
import org.matsim.core.router.speedy.SpeedyCHTTFCustomizer;
import org.matsim.core.router.speedy.SpeedyGraph;
import org.matsim.core.router.speedy.SpeedyGraphBuilder;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;
import org.matsim.core.trafficmonitoring.FreeSpeedTravelTime;

/**
 * Tests for the SpeedyCH-based DRT insertion one-to-many path search.
 * <p>
 * Verifies that {@link MultiInsertionDetourPathCalculatorManager} correctly creates
 * CH-accelerated calculators when {@code useSpeedyCHForInsertionSearch=true}
 * in {@link DrtConfigGroup}, and that the travel times match the Dijkstra baseline.
 *
 * @author Steffen Axer
 */
public class MultiInsertionDetourPathCalculatorCHTest {

	private static final double FREESPEED = 15.0; // m/s
	private static final double LINK_LENGTH = 150.0; // m → TT = 10 s per link

	/**
	 * Builds a simple A→B→C→D→E linear network with free-speed 15 m/s links.
	 */
	private static Network buildLinearNetwork() {
		Network network = NetworkUtils.createNetwork();
		NetworkFactory nf = network.getFactory();

		Node a = nf.createNode(Id.createNodeId("A"), new Coord(0, 0));
		Node b = nf.createNode(Id.createNodeId("B"), new Coord(150, 0));
		Node c = nf.createNode(Id.createNodeId("C"), new Coord(300, 0));
		Node d = nf.createNode(Id.createNodeId("D"), new Coord(450, 0));
		Node e = nf.createNode(Id.createNodeId("E"), new Coord(600, 0));

		for (Node n : List.of(a, b, c, d, e)) network.addNode(n);

		addLink(network, "AB", a, b);
		addLink(network, "BC", b, c);
		addLink(network, "CD", c, d);
		addLink(network, "DE", d, e);
		// reverse direction for backward search
		addLink(network, "BA", b, a);
		addLink(network, "CB", c, b);
		addLink(network, "DC", d, c);
		addLink(network, "ED", e, d);

		return network;
	}

	private static void addLink(Network network, String id, Node from, Node to) {
		NetworkFactory nf = network.getFactory();
		Link link = nf.createLink(Id.createLinkId(id), from, to);
		link.setLength(LINK_LENGTH);
		link.setFreespeed(FREESPEED);
		link.setCapacity(1800);
		link.setNumberOfLanes(1);
		network.addLink(link);
	}

	/**
	 * Verifies that the CH-based one-to-many path search returns the same travel
	 * time as the Dijkstra-based baseline on a small linear network.
	 */
	@Test
	void chBasedPathSearchMatchesDijkstra() {
		Network network = buildLinearNetwork();
		TravelTime travelTime = new FreeSpeedTravelTime();
		TravelDisutility travelDisutility = new TimeAsTravelDisutility(travelTime);

		// Build Dijkstra-based search (baseline)
		SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);
		OneToManyPathSearch dijkstraSearch = OneToManyPathSearch.createSearch(baseGraph, travelTime, travelDisutility, false);

		// Build CH-based search
		SpeedyGraph chBaseGraph = SpeedyGraphBuilder.build(network);
		InertialFlowCutter.NDOrderResult ndOrder = new InertialFlowCutter(chBaseGraph).computeOrderWithBatches();
		SpeedyCHGraph chGraph = new SpeedyCHBuilder(chBaseGraph, travelDisutility).buildWithOrderParallel(ndOrder);
		new SpeedyCHTTFCustomizer().customize(chGraph, travelTime, travelDisutility);
		OneToManyPathSearch chSearch = OneToManyPathSearch.createSearchCH(chGraph, travelTime, travelDisutility, false);

		Link linkAB = network.getLinks().get(Id.createLinkId("AB"));
		Link linkBC = network.getLinks().get(Id.createLinkId("BC"));
		Link linkCD = network.getLinks().get(Id.createLinkId("CD"));
		Link linkDE = network.getLinks().get(Id.createLinkId("DE"));
		Link linkBA = network.getLinks().get(Id.createLinkId("BA"));
		Link linkCB = network.getLinks().get(Id.createLinkId("CB"));
		Link linkDC = network.getLinks().get(Id.createLinkId("DC"));

		// Forward: from linkAB to linkDE, linkCD
		double startTime = 8.0 * 3600;
		var dijkstraResults = dijkstraSearch.calcPathDataArray(linkAB, List.of(linkBC, linkCD, linkDE), startTime, true);
		var chResults = chSearch.calcPathDataArray(linkAB, List.of(linkBC, linkCD, linkDE), startTime, true);

		assertThat(chResults).hasSameSizeAs(dijkstraResults);
		for (int i = 0; i < dijkstraResults.length; i++) {
			assertThat(chResults[i].getTravelTime())
					.as("forward travel time mismatch at index " + i)
					.isCloseTo(dijkstraResults[i].getTravelTime(), org.assertj.core.data.Offset.offset(1e-6));
		}

		// Backward: from linkDE to linkDC, linkCB, linkBA
		var dijkstraBackResults = dijkstraSearch.calcPathDataArray(linkDE, List.of(linkDC, linkCB, linkBA), startTime, false);
		var chBackResults = chSearch.calcPathDataArray(linkDE, List.of(linkDC, linkCB, linkBA), startTime, false);

		assertThat(chBackResults).hasSameSizeAs(dijkstraBackResults);
		for (int i = 0; i < dijkstraBackResults.length; i++) {
			assertThat(chBackResults[i].getTravelTime())
					.as("backward travel time mismatch at index " + i)
					.isCloseTo(dijkstraBackResults[i].getTravelTime(), org.assertj.core.data.Offset.offset(1e-6));
		}
	}

	/**
	 * Verifies that {@link DrtConfigGroup#isUseSpeedyCHForInsertionSearch()} defaults
	 * to false and can be set to true.
	 */
	@Test
	void drtConfigGroupFlagDefaultsFalse() {
		DrtConfigGroup cfg = new DrtConfigGroup();
		assertThat(cfg.isUseSpeedyCHForInsertionSearch()).isFalse();
	}

	@Test
	void drtConfigGroupFlagCanBeEnabled() {
		DrtConfigGroup cfg = new DrtConfigGroup();
		cfg.setUseSpeedyCHForInsertionSearch(true);
		assertThat(cfg.isUseSpeedyCHForInsertionSearch()).isTrue();
	}

	/**
	 * Smoke test: {@link MultiInsertionDetourPathCalculatorManager} successfully
	 * creates a CH-based calculator when the config flag is set.
	 */
	@Test
	void managerCreatesCHCalculatorWhenFlagSet() {
		Network network = buildLinearNetwork();
		TravelTime travelTime = new FreeSpeedTravelTime();
		FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

		DrtConfigGroup drtCfg = new DrtConfigGroup();
		drtCfg.setUseSpeedyCHForInsertionSearch(true);

		var manager = new MultiInsertionDetourPathCalculatorManager(network, travelTime, tc, drtCfg);
		// Should not throw
		var calculator = manager.create();
		assertThat(calculator).isNotNull();
	}
}
