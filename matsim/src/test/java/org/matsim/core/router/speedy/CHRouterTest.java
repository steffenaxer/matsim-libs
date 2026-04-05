package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.router.AbstractCHLeastCostPathCalculatorTest;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;

/**
 * Standard LeastCostPathCalculator test suite for the CHRouter system.
 * Uses {@link CHRouterTimeDep} with constant {@link FreespeedTravelTimeAndDisutility}
 * TTFs so that the time-dependent router produces static shortest paths matching
 * the expected test results.
 */
public class CHRouterTest extends AbstractCHLeastCostPathCalculatorTest {

    @Override
    protected LeastCostPathCalculator getLeastCostPathCalculator(final Network network) {
        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        CHGraph chGraph = new CHBuilder(g, tc).build();
        new CHTTFCustomizer().customize(chGraph, tc, tc);
        return new CHRouterTimeDep(chGraph, tc, tc);
    }
}
