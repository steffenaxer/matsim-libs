package org.matsim.core.router.speedy;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
 * <p>Follows the CCH (Customizable Contraction Hierarchies) paradigm where the
 * expensive contraction is performed once and then the CH overlay graph is merely
 * <em>customized</em> (edge-weight assignment) on subsequent calls. This dramatically
 * reduces repeated preprocessing cost because:
 * <ol>
 *   <li>The base {@link SpeedyGraph} is cached per {@link Network}.</li>
 *   <li>The contracted {@link SpeedyCHGraph} <em>skeleton</em> is cached per
 *       (network, TravelDisutility) pair. The contraction topology (node ordering +
 *       shortcuts) depends only on the cost function's lower bounds.</li>
 *   <li>On each {@link #createPathCalculator} call only the O(m) customization steps
 *       run: {@link SpeedyCHTTFCustomizer} for time-dependent TTFs, or
 *       {@link SpeedyCHCustomizer} for a static variant.</li>
 * </ol>
 *
 * <p>This class is {@link Singleton} and thread-safe: every call returns a new,
 * independent {@link SpeedyCHTimeDep} instance. The underlying CH structure may be
 * shared, but the query objects are per-thread.
 *
 * @author Implementation for CCH/CATCHUp router
 */
@Singleton
public class SpeedyCHFactory implements LeastCostPathCalculatorFactory {

    private static final Logger LOG = LogManager.getLogger(SpeedyCHFactory.class);

    private final Map<Network, SpeedyGraph> baseGraphs = new ConcurrentHashMap<>();

    /**
     * Cache keyed by (SpeedyGraph, TravelDisutility identity) → contracted CH graph.
     * Using identity-based keys because the same disutility object is typically reused
     * across iterations in MATSim, and its metric doesn't change between calls.
     */
    private final Map<CHCacheKey, SpeedyCHGraph> chGraphCache = new ConcurrentHashMap<>();

    @Inject
    public SpeedyCHFactory() {
    }

    @Override
    public LeastCostPathCalculator createPathCalculator(
            Network network, TravelDisutility travelCosts, TravelTime travelTimes) {

        SpeedyGraph baseGraph = baseGraphs.computeIfAbsent(network, SpeedyGraphBuilder::build);

        // Look up or build the contracted CH graph (expensive; cached).
        CHCacheKey cacheKey = new CHCacheKey(baseGraph, travelCosts);
        SpeedyCHGraph chGraph = chGraphCache.computeIfAbsent(cacheKey, key -> {
            LOG.info("Building CH contraction for network ({} nodes, {} links) – this is a one-time cost.",
                    baseGraph.nodeCount, baseGraph.linkCount);
            InertialFlowCutter.NDOrderResult ndOrder =
                    new InertialFlowCutter(baseGraph).computeOrderWithBatches();
            return new SpeedyCHBuilder(baseGraph, travelCosts).buildWithOrderParallel(ndOrder);
        });

        // Customise with time-dependent TTFs (fast O(edges × bins) pass).
        new SpeedyCHTTFCustomizer().customize(chGraph, travelTimes, travelCosts);

        return new SpeedyCHTimeDep(chGraph, travelTimes, travelCosts);
    }

    /** Composite cache key based on identity of the base graph and the travel disutility. */
    private record CHCacheKey(SpeedyGraph graph, TravelDisutility td) {}
}

