package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.router.AbstractCHLeastCostPathCalculatorTest;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;

/**
 * Standard LeastCostPathCalculator test suite for the SpeedyCH system
 * (CATCHUp time-dependent router backed by {@link SpeedyCHTimeDep}).
 *
 * <p>Uses {@link FreespeedTravelTimeAndDisutility} which makes TTFs constant over
 * time, so results must match the static shortest paths expected by the test suite.
 */
public class SpeedyCHTest extends AbstractCHLeastCostPathCalculatorTest {

    @Override
    protected LeastCostPathCalculator getLeastCostPathCalculator(final Network network) {
        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        SpeedyCHGraph chGraph = new SpeedyCHBuilder(g, tc).build();
        new SpeedyCHTTFCustomizer().customize(chGraph, tc, tc);
        return new SpeedyCHTimeDep(chGraph, tc, tc);
    }
}
