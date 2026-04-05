package org.matsim.contrib.drt.optimizer.insertion.extensive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.core.mobsim.framework.events.MobsimBeforeCleanupEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimBeforeCleanupListener;
import org.matsim.core.router.speedy.InertialFlowCutter;
import org.matsim.core.router.speedy.SpeedyCHBuilder;
import org.matsim.core.router.speedy.SpeedyCHGraph;
import org.matsim.core.router.speedy.SpeedyCHTTFCustomizer;
import org.matsim.core.router.speedy.SpeedyGraph;
import org.matsim.core.router.speedy.SpeedyGraphBuilder;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultiInsertionDetourPathCalculatorManager implements MobsimBeforeCleanupListener {

	private static final Logger LOG = LogManager.getLogger(MultiInsertionDetourPathCalculatorManager.class);

	/**
	 * Static, cross-instance cache for fully built and TTF-customized CH graphs.
	 * <p>
	 * Keyed by (Network, TravelDisutility, TravelTime) identity.  When multiple
	 * DRT modes share the same network and cost functions (the typical multi-mode
	 * DRT setup), the expensive CH build + TTF customization is performed only once.
	 * <p>
	 * The {@link SpeedyCHGraph} is read-only after customization: all per-query
	 * mutable state lives in the individual {@link org.matsim.core.router.speedy.SpeedyCHLeastCostPathTree}
	 * instances created by {@link org.matsim.contrib.dvrp.path.OneToManyPathSearch#createSearchCH},
	 * so sharing the graph across threads is safe.
	 */
	private static final Map<CHCacheKey, SpeedyCHGraph> CH_GRAPH_CACHE = new ConcurrentHashMap<>();

	private final Network network;
	private final TravelTime travelTime;
	private final TravelDisutility travelDisutility;
	private final DrtConfigGroup drtCfg;
	private final List<MultiInsertionDetourPathCalculator> multiInsertionDetourPathCalculatorList;

	MultiInsertionDetourPathCalculatorManager(Network network, TravelTime travelTime, TravelDisutility travelDisutility,
											  DrtConfigGroup drtCfg)
	{
		this.network = network;
		this.travelTime = travelTime;
		this.travelDisutility = travelDisutility;
		this.drtCfg = drtCfg;
		this.multiInsertionDetourPathCalculatorList = new ArrayList<>();
	}

	@Override
	public void notifyMobsimBeforeCleanup(MobsimBeforeCleanupEvent e) {
		multiInsertionDetourPathCalculatorList.forEach(i -> i.notifyMobsimBeforeCleanup(e));
	}

	MultiInsertionDetourPathCalculator create()
	{
		MultiInsertionDetourPathCalculator instance;
		if (drtCfg.isUseSpeedyCHForInsertionSearch()) {
			instance = new MultiInsertionDetourPathCalculator(getOrBuildCHGraph(), travelTime, travelDisutility, drtCfg);
		} else {
			instance = new MultiInsertionDetourPathCalculator(network, travelTime, travelDisutility, drtCfg);
		}
		this.multiInsertionDetourPathCalculatorList.add(instance);
		return instance;
	}

	/**
	 * Returns a shared CH graph for this (network, disutility, travelTime) triple,
	 * building it on first access.  The graph is cached in a static map so that
	 * multiple manager instances (e.g. from different DRT modes on the same network)
	 * share a single CH graph.
	 */
	private SpeedyCHGraph getOrBuildCHGraph() {
		CHCacheKey key = new CHCacheKey(network, travelDisutility, travelTime);
		return CH_GRAPH_CACHE.computeIfAbsent(key, k -> {
			LOG.info("Building SpeedyCH graph for DRT insertion search on network with {} nodes, {} links – one-time cost.",
					network.getNodes().size(), network.getLinks().size());
			SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);
			InertialFlowCutter.NDOrderResult ndOrder = new InertialFlowCutter(baseGraph).computeOrderWithBatches();
			SpeedyCHGraph chGraph = new SpeedyCHBuilder(baseGraph, travelDisutility).buildWithOrderParallel(ndOrder);
			new SpeedyCHTTFCustomizer().customize(chGraph, travelTime, travelDisutility);
			LOG.info("SpeedyCH graph built for DRT insertion search (base: {} links).",
					network.getLinks().size());
			return chGraph;
		});
	}

	/**
	 * Composite cache key using identity equality for all three components.
	 * Two managers sharing the same Network, TravelDisutility, and TravelTime
	 * objects will produce equal keys and thus share the same CH graph.
	 */
	private record CHCacheKey(Network network, TravelDisutility td, TravelTime tt) {}
}
