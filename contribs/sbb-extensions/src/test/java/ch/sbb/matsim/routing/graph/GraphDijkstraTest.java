package ch.sbb.matsim.routing.graph;

import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.groups.PlanCalcScoreConfigGroup;
import org.matsim.core.router.AbstractLeastCostPathCalculatorTest;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;

/**
 * @author mrieser / Simunto
 */
public class GraphDijkstraTest extends AbstractLeastCostPathCalculatorTest {

	@Override
	protected LeastCostPathCalculator getLeastCostPathCalculator(Network network) {
		Graph graph = new Graph(network);
		FreespeedTravelTimeAndDisutility travelTimeCostCalculator = new FreespeedTravelTimeAndDisutility(new PlanCalcScoreConfigGroup());
		return new GraphDijkstra(graph, travelTimeCostCalculator, travelTimeCostCalculator);
	}
}