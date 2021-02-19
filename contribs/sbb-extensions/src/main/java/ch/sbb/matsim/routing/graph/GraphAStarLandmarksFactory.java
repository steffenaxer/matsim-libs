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
public class GraphAStarLandmarksFactory implements LeastCostPathCalculatorFactory {

	private final Map<Network, Graph> graphs = new ConcurrentHashMap<>();
	private final Map<Graph, GraphAStarLandmarksData> landmarksData = new ConcurrentHashMap<>();

	@Override
	public LeastCostPathCalculator createPathCalculator(Network network, TravelDisutility travelCosts, TravelTime travelTimes) {
		Graph graph = this.graphs.get(network);
		if (graph == null) {
			graph = new Graph(network);
			this.graphs.put(network, graph);
		}
		GraphAStarLandmarksData landmarks = this.landmarksData.get(graph);
		if (landmarks == null) {
			landmarks = new GraphAStarLandmarksData(graph, 16, travelCosts);
			this.landmarksData.put(graph, landmarks);
		}
		return new GraphAStarLandmarks(landmarks, travelTimes, travelCosts);
	}

}
