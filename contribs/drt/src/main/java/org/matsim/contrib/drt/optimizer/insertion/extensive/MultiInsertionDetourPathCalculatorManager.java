package org.matsim.contrib.drt.optimizer.insertion.extensive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.core.mobsim.framework.events.MobsimBeforeCleanupEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimBeforeCleanupListener;
import org.matsim.core.router.speedy.InertialFlowCutter;
import org.matsim.core.router.speedy.CHBuilder;
import org.matsim.core.router.speedy.CHGraph;
import org.matsim.core.router.speedy.CHTTFCustomizer;
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
	 * The {@link CHGraph} is read-only after customization: all per-query
	 * mutable state lives in the individual {@link org.matsim.core.router.speedy.CHLeastCostPathTree}
	 * instances created by {@link org.matsim.contrib.dvrp.path.OneToManyPathSearch#createSearchCH},
	 * so sharing the graph across threads is safe.
	 */
	private static final Map<CHCacheKey, CHGraph> CH_GRAPH_CACHE = new ConcurrentHashMap<>();

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
		if (drtCfg.isUseCHForInsertionSearch()) {
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
	private CHGraph getOrBuildCHGraph() {
		CHCacheKey key = new CHCacheKey(network, travelDisutility, travelTime);
		return CH_GRAPH_CACHE.computeIfAbsent(key, k -> {
			LOG.info("Building CHRouter graph for DRT insertion search on network with {} nodes, {} links – one-time cost.",
					network.getNodes().size(), network.getLinks().size());
			SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);
			InertialFlowCutter.NDOrderResult ndOrder = new InertialFlowCutter(baseGraph).computeOrderWithBatches();
			CHGraph chGraph = new CHBuilder(baseGraph, travelDisutility).buildWithOrderParallel(ndOrder);
			new CHTTFCustomizer().customize(chGraph, travelTime, travelDisutility);
			LOG.info("CHRouter graph built for DRT insertion search (base: {} links).",
					network.getLinks().size());
			return chGraph;
		});
	}

	/**
	 * Composite cache key using <em>reference equality</em> (not
	 * {@code .equals()}) for all three components.  Two managers sharing
	 * the same {@code Network}, {@code TravelDisutility}, and
	 * {@code TravelTime} <b>object instances</b> will produce equal keys
	 * and thus share the same CH graph.  Managers with structurally
	 * identical but distinct objects will build separate CH graphs.
	 */
	private record CHCacheKey(Network network, TravelDisutility td, TravelTime tt) {}
}
