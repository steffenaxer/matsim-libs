package ch.sbb.matsim.routing.graph;

import org.matsim.api.core.v01.network.Network;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.LeastCostPathCalculatorFactory;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mrieser / Simunto
 */
public class GraphDijkstraFactory implements LeastCostPathCalculatorFactory {

	private final Map<Network, Graph> graphs = new ConcurrentHashMap<>();

	@Override
	public LeastCostPathCalculator createPathCalculator(Network network, TravelDisutility travelCosts, TravelTime travelTimes) {
		Graph graph = graphs.get(network);
		if (graph == null) {
			graph = new Graph(network);
			graphs.put(network, graph);
		}
		return new GraphDijkstra(graph, travelTimes, travelCosts);
	}
}
