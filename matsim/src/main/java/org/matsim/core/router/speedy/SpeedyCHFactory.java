package org.matsim.core.router.speedy;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.LeastCostPathCalculatorFactory;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for {@link SpeedyCH} instances.
 *
 * <p>The base {@link SpeedyGraph} is cached per {@link Network}.
 * The CH contraction and customization are performed on every
 * {@link #createPathCalculator} call (re-customization strategy, Phase 1).
 * Caching of the CH structure keyed by (network, travelDisutility) is a
 * future optimisation.
 *
 * <p>This class is {@link Singleton} and thread-safe: every call returns a
 * fresh, non-shared {@link SpeedyCH} instance.
 *
 * @author Implementation for CCH/CATCHUp router
 */
@Singleton
public class SpeedyCHFactory implements LeastCostPathCalculatorFactory {

    private final Map<Network, SpeedyGraph> baseGraphs = new ConcurrentHashMap<>();

    @Inject
    public SpeedyCHFactory() {
    }

    @Override
    public LeastCostPathCalculator createPathCalculator(
            Network network, TravelDisutility travelCosts, TravelTime travelTimes) {

        SpeedyGraph baseGraph = baseGraphs.computeIfAbsent(network, SpeedyGraphBuilder::build);

        SpeedyCHGraph chGraph = new SpeedyCHBuilder(baseGraph, travelCosts).build();
        new SpeedyCHCustomizer().customize(chGraph, travelCosts);

        return new SpeedyCH(chGraph, travelTimes, travelCosts);
    }
}
