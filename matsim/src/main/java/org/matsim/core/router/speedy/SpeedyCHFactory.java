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
 * Factory for the time-dependent CATCHUp router ({@link SpeedyCHTimeDep}).
 *
 * <p>The base {@link SpeedyGraph} is cached per {@link Network}.  On every
 * {@link #createPathCalculator} call the CH is contracted with static lower-bound
 * weights ({@link SpeedyCHBuilder}) and then time-dependently customised
 * ({@link SpeedyCHTTFCustomizer}) before a fresh {@link SpeedyCHTimeDep} instance
 * is returned.
 *
 * <p>This class is {@link Singleton} and thread-safe: every call returns a new,
 * independent {@link SpeedyCHTimeDep} instance backed by a newly customised graph.
 * Caching of the CH structure keyed by (network, travelCosts) is a planned
 * future optimisation.
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

        // Build CH structure (contraction uses static lower-bound weights).
        SpeedyCHGraph chGraph = new SpeedyCHBuilder(baseGraph, travelCosts).build();

        // Customise with time-dependent TTFs.
        new SpeedyCHTTFCustomizer().customize(chGraph, travelTimes, travelCosts);

        return new SpeedyCHTimeDep(chGraph, travelTimes, travelCosts);
    }
}

