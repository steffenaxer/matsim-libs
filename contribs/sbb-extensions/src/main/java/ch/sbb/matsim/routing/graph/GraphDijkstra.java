package ch.sbb.matsim.routing.graph;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Node;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;
import org.matsim.core.utils.misc.OptionalTime;
import org.matsim.vehicles.Vehicle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author mrieser / Simunto
 */
public class GraphDijkstra implements LeastCostPathCalculator {

	private final Graph graph;
	private final TravelTime tt;
	private final TravelDisutility td;
	private final double[] data; // 3 entries per node: time, cost, distance
	private final int[] comingFrom;
	private final int[] usedLink;
	private final Graph.LinkIterator outLI;
	private final Graph.LinkIterator inLI;
	private final DAryMinHeap pq;

	public GraphDijkstra(Graph graph, TravelTime tt, TravelDisutility td) {
		this.graph = graph;
		this.tt = tt;
		this.td = td;
		this.data = new double[graph.nodeCount * 3];
		this.comingFrom = new int[graph.nodeCount];
		this.usedLink = new int[graph.nodeCount];
		this.pq = new DAryMinHeap(graph.nodeCount, 6, this::getCost, this::setCost);
		this.outLI = graph.getOutLinkIterator();
		this.inLI = graph.getInLinkIterator();
	}

	public double getCost(int nodeIndex) {
		return this.data[nodeIndex * 3];
	}

	private double getTimeRaw(int nodeIndex) {
		return this.data[nodeIndex * 3 + 1];
	}

	public OptionalTime getTime(int nodeIndex) {
		double time = this.data[nodeIndex * 3 + 1];
		if (Double.isInfinite(time)) {
			return OptionalTime.undefined();
		}
		return OptionalTime.defined(time);
	}

	public double getDistance(int nodeIndex) {
		return this.data[nodeIndex * 3 + 2];
	}

	private void setCost(int nodeIndex, double cost) {
		this.data[nodeIndex * 3] = cost;
	}

	private void setData(int nodeIndex, double cost, double time, double distance) {
		int index = nodeIndex * 3;
		this.data[index] = cost;
		this.data[index + 1] = time;
		this.data[index + 2] = distance;
	}

	@Override
	public Path calcLeastCostPath(Node startNode, Node endNode, double startTime, Person person, Vehicle vehicle) {
		int startNodeIndex = startNode.getId().index();
		int endNodeIndex = endNode.getId().index();

		Arrays.fill(this.data, Double.POSITIVE_INFINITY);
		Arrays.fill(this.comingFrom, -1);

		setData(startNodeIndex, 0, startTime, 0);
		this.pq.clear();
		this.pq.insert(startNodeIndex);
		boolean foundEndNode = false;

		while (!pq.isEmpty()) {
			final int nodeIdx = pq.poll();
			if (nodeIdx == endNodeIndex) {
				foundEndNode = true;
				break;
			}

			double currTime = getTimeRaw(nodeIdx);
			if (Double.isInfinite(currTime)) {
				throw new RuntimeException("Undefined Time");
			}
			double currCost = getCost(nodeIdx);
			double currDistance = getDistance(nodeIdx);

			outLI.reset(nodeIdx);
			while (outLI.next()) {
				int linkIdx = outLI.getLinkIndex();
				Link link = this.graph.getLink(linkIdx);
				int toNode = outLI.getToNodeIndex();

				double travelTime = this.tt.getLinkTravelTime(link, currTime, person, vehicle);
				double newTime = currTime + travelTime;
				double newCost = currCost + this.td.getLinkTravelDisutility(link, currTime, person, vehicle);

				double oldCost = getCost(toNode);
				if (Double.isFinite(oldCost)) {
					if (newCost < oldCost) {
						pq.decreaseKey(toNode, newCost);
						setData(toNode, newCost, newTime, currDistance + link.getLength());
						this.comingFrom[toNode] = nodeIdx;
						this.usedLink[toNode] = linkIdx;
					}
				} else {
					setData(toNode, newCost, newTime, currDistance + link.getLength());
					pq.insert(toNode);
					this.comingFrom[toNode] = nodeIdx;
					this.usedLink[toNode] = linkIdx;
				}
			}
		}

		if (foundEndNode) {
			return constructPath(endNodeIndex, startTime);
		}
		return null;
	}

	private Path constructPath(int endNodeIndex, double startTime) {
		double travelCost = getCost(endNodeIndex);
		double arrivalTime = getTimeRaw(endNodeIndex);
		if (Double.isInfinite(arrivalTime)) {
			throw new RuntimeException("Undefined time on end node");
		}
		double travelTime = arrivalTime - startTime;

		List<Node> nodes = new ArrayList<>();
		List<Link> links = new ArrayList<>();

		int nodeIndex = endNodeIndex;

		nodes.add(this.graph.getNode(nodeIndex));

		int linkIndex = this.usedLink[nodeIndex];
		nodeIndex = this.comingFrom[nodeIndex];

		while (nodeIndex >= 0) {
			nodes.add(this.graph.getNode(nodeIndex));
			links.add(this.graph.getLink(linkIndex));

			linkIndex = this.usedLink[nodeIndex];
			nodeIndex = this.comingFrom[nodeIndex];
		}

		Collections.reverse(nodes);
		Collections.reverse(links);

		return new Path(nodes, links, travelTime, travelCost);
	}
}
