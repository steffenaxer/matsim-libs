/* *********************************************************************** *
 * project: org.matsim.*
 * CHRouterTimeDepTest.java
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

package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.router.AbstractCHLeastCostPathCalculatorTest;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;

/**
 * Tests for the time-dependent CATCHUp router ({@link CHRouterTimeDep}) using the
 * standard test suite with relaxed tie-breaking for equal-cost paths.
 *
 * <p>FreespeedTravelTimeAndDisutility is used as the TravelTime/TravelDisutility provider,
 * which makes the TTF constant over time - ensuring the time-dependent results match
 * the static shortest paths expected by the test suite.
 *
 * @author Steffen Axer
 */
public class CHRouterTimeDepTest extends AbstractCHLeastCostPathCalculatorTest {

    @Override
    protected LeastCostPathCalculator getLeastCostPathCalculator(final Network network) {
        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        CHGraph chGraph = new CHBuilder(g, tc).build();
        new CHTTFCustomizer().customize(chGraph, tc, tc);
        return new CHRouterTimeDep(chGraph, tc, tc);
    }
}
