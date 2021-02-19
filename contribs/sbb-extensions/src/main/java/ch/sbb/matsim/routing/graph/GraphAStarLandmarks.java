package ch.sbb.matsim.routing.graph;

import ch.sbb.matsim.routing.graph.DAryMinHeap.IntIterator;
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
public class GraphAStarLandmarks implements LeastCostPathCalculator {

	private final Graph graph;
	private final GraphAStarLandmarksData astarData;
	private final TravelTime tt;
	private final TravelDisutility td;
	private final double[] data; // 3 entries per node: cost to node, time, distance
	private final int[] comingFrom;
	private final int[] usedLink;
	private final Graph.LinkIterator outLI;
	private final DAryMinHeap pq;
//	private final boolean[] activeLandmarks;

	public GraphAStarLandmarks(GraphAStarLandmarksData astarData, TravelTime tt, TravelDisutility td) {
		this.graph = astarData.graph;
		this.astarData = astarData;
		this.tt = tt;
		this.td = td;
		this.data = new double[this.graph.nodeCount * 3];
		this.comingFrom = new int[this.graph.nodeCount];
		this.usedLink = new int[this.graph.nodeCount];
		this.pq = new DAryMinHeap(this.graph.nodeCount, 6);
		this.outLI = this.graph.getOutLinkIterator();
//		this.activeLandmarks = new boolean[this.astarData.getLandmarksCount()];
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

//		System.err.println("FROM " + startNodeIndex + " (" + this.graph.getNode(startNodeIndex).getId() + ")");
//		System.err.println("TO " + endNodeIndex + " (" + this.graph.getNode(endNodeIndex).getId() + ")");

		int startDeadend = this.astarData.getNodeDeadend(startNodeIndex);
		int endDeadend = this.astarData.getNodeDeadend(endNodeIndex);

		Arrays.fill(this.data, Double.POSITIVE_INFINITY);
		Arrays.fill(this.comingFrom, -1);
//		Arrays.fill(this.activeLandmarks, true);

//		double estimate = initActiveLandmarks(startNodeIndex, endNodeIndex);
		double estimation = estimateMinTravelcostToDestination(startNodeIndex, endNodeIndex, endNode,false);

		setData(startNodeIndex, 0, startTime, 0);
		this.pq.clear();
		this.pq.insert(startNodeIndex, estimation);
//		System.err.println("INSERT " + startNodeIndex + " (" + this.graph.getNode(startNodeIndex).getId() + ") cost = " + getCost(startNodeIndex) + "  pqCost = " + getHeapCost(startNodeIndex));
		boolean foundEndNode = false;

//		int counter = 0;
		while (!this.pq.isEmpty()) {
			final int nodeIdx = this.pq.poll();
//			System.err.println("POLL " + nodeIdx + " (" + this.graph.getNode(nodeIdx).getId() + ") cost = " + getCost(nodeIdx) + "  pqCost = " + getHeapCost(nodeIdx));
			if (nodeIdx == endNodeIndex) {
				foundEndNode = true;
				break;
			}

			// ignore deadends
//			int deadend = this.astarData.getNodeDeadend(nodeIdx);
//			if (deadend >= 0 && deadend != startDeadend && deadend != endDeadend) {
//				continue; // it's a dead-end we're not interested in
//			}

//			counter++;

			double currTime = getTimeRaw(nodeIdx);
			if (Double.isInfinite(currTime)) {
				throw new RuntimeException("Undefined Time");
			}
			double currCost = getCost(nodeIdx);
			double currDistance = getDistance(nodeIdx);

			this.outLI.reset(nodeIdx);
			while (this.outLI.next()) {
				int linkIdx = this.outLI.getLinkIndex();
				Link link = this.graph.getLink(linkIdx);
				int toNode = this.outLI.getToNodeIndex();

				double travelTime = this.tt.getLinkTravelTime(link, currTime, person, vehicle);
				double newTime = currTime + travelTime;
				double travelCost = this.td.getLinkTravelDisutility(link, currTime, person, vehicle);
//				System.err.println("   link " + link.getId() + " from " + link.getFromNode().getId().index() + " to " + link.getToNode().getId().index() + " tt = " + travelTime + " tc = " + travelCost);
				double newCost = currCost + travelCost;

				boolean resetLandmarks = false;//counter >= 40;
//				if (resetLandmarks) counter = 0;

				double oldCost = getCost(toNode);
				if (Double.isFinite(oldCost)) {
					if (newCost < oldCost) {
						estimation = estimateMinTravelcostToDestination(toNode, endNodeIndex, endNode, resetLandmarks);
//						double estimation = getHeapCost(toNode) - oldCost;
//						System.err.println("DECREASE " + toNode + " (" + this.graph.getNode(toNode).getId() + ") cost = " + getCost(toNode) + "  pqCost = " + getHeapCost(toNode) + " TO cost " + newCost + "  pqCost = " + (newCost + estimation));
						this.pq.decreaseKey(toNode, newCost + estimation);
						setData(toNode, newCost, newTime, currDistance + link.getLength());
						this.comingFrom[toNode] = nodeIdx;
						this.usedLink[toNode] = linkIdx;
					}
				} else {
					estimation = estimateMinTravelcostToDestination(toNode, endNodeIndex, endNode, resetLandmarks);
					setData(toNode, newCost, newTime, currDistance + link.getLength());
//					System.err.println("INSERT " + toNode + " (" + this.graph.getNode(toNode).getId() + ") cost = " + getCost(toNode) + "  pqCost = " + getHeapCost(toNode));
					this.pq.insert(toNode, newCost + estimation);
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

	private double initActiveLandmarks(int nodeIdx, int destinationIdx) {
		double[] estimates = new double[this.astarData.getLandmarksCount()];
		double best = -1;
//		int bestIdx = -1;
		for (int i = 0; i < estimates.length; i++) {
			estimates[i] = estimateMinTravelcostToDestination(nodeIdx, destinationIdx, i);
			if (estimates[i] > best) {
				best = estimates[i];
//				bestIdx = i;
			}
		}
//		this.activeLandmarks[bestIdx] = true;
//		// limit currently contains the largest value. Now find the next lower value, reduce the limit, and repeat,
//		// until we have the 4th-highest number.
//
//		double limit = best;
//		double newLimit = -1;
//		for (int round = 0; round < 3; round++) {
//			for (int i = 0; i < estimates.length; i++) {
//				if (estimates[i] < limit && estimates[i] > newLimit) {
//					newLimit = estimates[i];
//				}
//			}
//			limit = newLimit;
//		}
//		for (int i = 0; i < estimates.length; i++) {
//			if (estimates[i] >= limit) {
//				this.activeLandmarks[i] = true;
//			}
//		}
//		System.out.println(Arrays.toString(this.activeLandmarks));
		return best;
	}

	private double estimateMinTravelcostToDestination(int nodeIdx, int destinationIdx, Node destinationNode, boolean improveLandmarks) {
		/* The ALT algorithm uses two lower bounds for each Landmark:
		 * given: source node S, target node T, landmark L
		 * then, due to the triangle inequality:
		 *  1) ST + TL >= SL --> ST >= SL - TL
		 *  2) LS + ST >= LT --> ST >= LT - LS
		 * The algorithm is interested in the largest possible value of (SL-TL) and (LT-LS),
		 * as this gives the closest approximation for the minimal travel time required to
		 * go from S to T.
		 */

		// minCost = distance * factor
		// minCost = sqrt(distance * distance) * factor



//		Node node = this.graph.getNode(nodeIdx);
//		double distance = CoordUtils.calcEuclideanDistance(node.getCoord(), destinationNode.getCoord());
//		double distance = Math.abs(node.getCoord().getX() - destinationNode.getCoord().getX()) + Math.abs(node.getCoord().getY() - destinationNode.getCoord().getY());
//		double best = distance * this.astarData.getMinTravelCostPerLength();
		double best = 0;
		for (int i = 0; i < this.astarData.getLandmarksCount(); i++) {
//			if (this.activeLandmarks[i]) {
				double estimate = estimateMinTravelcostToDestination(nodeIdx, destinationIdx, i);
				if (estimate > best) {
					best = estimate;
				}
//			}
		}
//
//		if (improveLandmarks) {
////			System.out.println("IMPROVE");
//			int newBest = -1;
//			for (int i = 0; i < this.astarData.getLandmarksCount(); i++) {
//				if (!this.activeLandmarks[i]) {
//					double estimate = estimateMinTraveltimeToDestination(nodeIdx, destinationIdx, i);
//					if (estimate > best) {
//						best = estimate;
//						newBest = i;
//					}
//				}
//			}
//			if (newBest >= 0) {
//				this.activeLandmarks[newBest] = true;
//				updateEstimations(destinationIdx, newBest);
//			}
//		}

//		System.err.println("  estimate for " + nodeIdx + " = " + best);
		return best;
	}

	private void updateEstimations(int destinationIdx, int landmarkIdx) {
		ArrayList<Integer> nodesToUpdate = new ArrayList<>();
		IntIterator iter = this.pq.iterator();
		while (iter.hasNext()) {
			int node = iter.next();
			double newEstimation = estimateMinTravelcostToDestination(node, destinationIdx, landmarkIdx);
			double cost = getCost(node);
			double heapCost = getHeapCost(node);
			double oldEstimation = heapCost - cost;
			if (newEstimation > oldEstimation) {
				nodesToUpdate.add(node);
			}
		}
		for (Integer node : nodesToUpdate) {
			this.pq.remove(node);
		}
		for (Integer node : nodesToUpdate) {
			double cost = getCost(node);
			double newEstimation = estimateMinTravelcostToDestination(node, destinationIdx, landmarkIdx);
			setHeapCost(node, cost + newEstimation);
			this.pq.insert(node);
		}
	}

	private double estimateMinTravelcostToDestination(int nodeIdx, int destinationIdx, int landmarkIdx) {
		double sl = this.astarData.getTravelCostToLandmark(nodeIdx, landmarkIdx);
		double ls = this.astarData.getTravelCostFromLandmark(nodeIdx, landmarkIdx);
		double tl = this.astarData.getTravelCostToLandmark(destinationIdx, landmarkIdx);
		double lt = this.astarData.getTravelCostFromLandmark(destinationIdx, landmarkIdx);
		double sltl = sl - tl;
		double ltls = lt - ls;
		return Math.max(sltl, ltls);
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
