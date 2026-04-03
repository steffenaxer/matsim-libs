package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.router.AbstractLeastCostPathCalculatorTestWithTurnRestrictions;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;

/**
 * Tests for {@link SpeedyCH} using the standard
 * {@link AbstractLeastCostPathCalculatorTestWithTurnRestrictions} test suite.
 */
public class SpeedyCHTest extends AbstractLeastCostPathCalculatorTestWithTurnRestrictions {

    @Override
    protected LeastCostPathCalculator getLeastCostPathCalculator(final Network network) {
        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        SpeedyCHGraph chGraph = new SpeedyCHBuilder(g, tc).build();
        new SpeedyCHCustomizer().customize(chGraph, tc);
        return new SpeedyCH(chGraph, tc, tc);
    }
}
