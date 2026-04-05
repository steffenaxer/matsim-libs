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

public class MultiInsertionDetourPathCalculatorManager implements MobsimBeforeCleanupListener {

	private static final Logger LOG = LogManager.getLogger(MultiInsertionDetourPathCalculatorManager.class);

	private final Network network;
	private final TravelTime travelTime;
	private final TravelDisutility travelDisutility;
	private final DrtConfigGroup drtCfg;
	private final List<MultiInsertionDetourPathCalculator> multiInsertionDetourPathCalculatorList;

	/**
	 * Lazily built CH graph, shared across all calculator instances.
	 * Non-null only when {@code drtCfg.isUseSpeedyCHForInsertionSearch()} is true.
	 */
	private SpeedyCHGraph chGraph;

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
	 * Builds (once) or returns the shared CH graph for this network.
	 * The graph is customized with time-dependent TTFs before first use.
	 */
	private synchronized SpeedyCHGraph getOrBuildCHGraph() {
		if (chGraph == null) {
			LOG.info("Building SpeedyCH graph for DRT insertion search on network with {} nodes, {} links – one-time cost.",
					network.getNodes().size(), network.getLinks().size());
			SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);
			InertialFlowCutter.NDOrderResult ndOrder = new InertialFlowCutter(baseGraph).computeOrderWithBatches();
			chGraph = new SpeedyCHBuilder(baseGraph, travelDisutility).buildWithOrderParallel(ndOrder);
			new SpeedyCHTTFCustomizer().customize(chGraph, travelTime, travelDisutility);
			LOG.info("SpeedyCH graph built for DRT insertion search (base: {} links).",
					network.getLinks().size());
		}
		return chGraph;
	}
}
