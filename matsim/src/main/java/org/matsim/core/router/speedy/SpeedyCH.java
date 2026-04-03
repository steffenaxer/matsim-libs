package org.matsim.core.router.speedy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Node;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.network.turnRestrictions.TurnRestrictionsContext;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;
import org.matsim.vehicles.Vehicle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Bidirectional Contraction Hierarchies (CH) shortest-path query.
 *
 * <ul>
 *   <li><b>Forward search</b> from source: relaxes upward out-edges only
 *       ({@link SpeedyCHGraph.UpwardOutEdgeIterator}).</li>
 *   <li><b>Backward search</b> from target: relaxes downward in-edges in reverse
 *       ({@link SpeedyCHGraph.DownwardInEdgeIterator}).</li>
 *   <li>The <em>meeting node</em> minimises {@code fwdCost[v] + bwdCost[v]}.</li>
 *   <li>Shortcuts are unpacked recursively via middleNode / lowerEdge1 / lowerEdge2.</li>
 * </ul>
 *
 * <p>This implementation is <b>not thread-safe</b>. Every thread must use its own instance,
 * but the underlying {@link SpeedyCHGraph} may be shared.
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCH implements LeastCostPathCalculator {

    private static final Logger LOG = LogManager.getLogger(SpeedyCH.class);

    private final SpeedyCHGraph chGraph;
    private final SpeedyGraph   baseGraph;
    private final TravelTime       tt;
    private final TravelDisutility td;

    // Forward-search per-node data
    private final double[] fwdCost;
    private final int[]    fwdComingFrom;
    private final int[]    fwdUsedEdge;
    private final int[]    fwdIterIds;

    // Backward-search per-node data
    private final double[] bwdCost;
    private final int[]    bwdComingFrom;
    private final int[]    bwdUsedEdge;
    private final int[]    bwdIterIds;

    private int currentIteration = Integer.MIN_VALUE;

    private final DAryMinHeap fwdPQ;
    private final DAryMinHeap bwdPQ;

    private final SpeedyCHGraph.UpwardOutEdgeIterator  outUpIter;
    private final SpeedyCHGraph.DownwardInEdgeIterator downInIter;

    public SpeedyCH(SpeedyCHGraph chGraph, TravelTime tt, TravelDisutility td) {
        this.chGraph   = chGraph;
        this.baseGraph = chGraph.getBaseGraph();
        this.tt        = tt;
        this.td        = td;

        int n = chGraph.nodeCount;
        this.fwdCost       = new double[n];
        this.fwdComingFrom = new int[n];
        this.fwdUsedEdge   = new int[n];
        this.fwdIterIds    = new int[n];

        this.bwdCost       = new double[n];
        this.bwdComingFrom = new int[n];
        this.bwdUsedEdge   = new int[n];
        this.bwdIterIds    = new int[n];

        Arrays.fill(fwdIterIds, currentIteration);
        Arrays.fill(bwdIterIds, currentIteration);

        this.fwdPQ = new DAryMinHeap(n, 6);
        this.bwdPQ = new DAryMinHeap(n, 6);

        this.outUpIter  = chGraph.getUpwardOutEdgeIterator();
        this.downInIter = chGraph.getDownwardInEdgeIterator();
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    @Override
    public Path calcLeastCostPath(Node startNode, Node endNode,
                                  double startTime, Person person, Vehicle vehicle) {
        int startIdx = startNode.getId().index();
        int endIdx   = endNode.getId().index();
        Path path    = calcLeastCostPathImpl(startIdx, endIdx, startTime, person, vehicle);
        if (path == null) {
            logNoRoute("node " + startNode.getId(), "node " + endNode.getId());
        }
        return path;
    }

    @Override
    public Path calcLeastCostPath(Link fromLink, Link toLink,
                                  double startTime, Person person, Vehicle vehicle) {
        int startIdx = fromLink.getToNode().getId().index();
        int endIdx   = toLink.getFromNode().getId().index();

        if (baseGraph.getTurnRestrictions().isPresent()) {
            Map<Id<Link>, TurnRestrictionsContext.ColoredLink> replaced =
                    baseGraph.getTurnRestrictions().get().replacedLinks;
            if (replaced.containsKey(fromLink.getId())) {
                startIdx = replaced.get(fromLink.getId()).toColoredNode.index();
            }
        }

        Path path = calcLeastCostPathImpl(startIdx, endIdx, startTime, person, vehicle);
        if (path == null) {
            logNoRoute("link " + fromLink.getId(), "link " + toLink.getId());
        }
        return path;
    }

    // -------------------------------------------------------------------------
    // Bidirectional CH query
    // -------------------------------------------------------------------------

    private Path calcLeastCostPathImpl(int startIdx, int endIdx,
                                       double startTime, Person person, Vehicle vehicle) {
        advanceIteration();

        // Trivial case
        if (startIdx == endIdx) {
            return new Path(
                    Collections.singletonList(baseGraph.getNode(startIdx)),
                    Collections.emptyList(), 0.0, 0.0);
        }

        // Initialize forward search
        setFwd(startIdx, 0.0, -1, -1);
        fwdPQ.clear();
        fwdPQ.insert(startIdx, 0.0);

        // Initialize backward search
        setBwd(endIdx, 0.0, -1, -1);
        bwdPQ.clear();
        bwdPQ.insert(endIdx, 0.0);

        // With turn restrictions, also seed backward search from colored copies of the
        // destination node and forward search from colored copies of the source node.
        Optional<TurnRestrictionsContext> trOpt = baseGraph.getTurnRestrictions();
        if (trOpt.isPresent()) {
            for (TurnRestrictionsContext.ColoredNode cn : trOpt.get().coloredNodes) {
                int origIdx = cn.node().getId().index();
                int coloredIdx = cn.index();
                if (origIdx == endIdx && coloredIdx != endIdx) {
                    setBwd(coloredIdx, 0.0, -1, -1);
                    bwdPQ.insert(coloredIdx, 0.0);
                }
                if (origIdx == startIdx && coloredIdx != startIdx) {
                    setFwd(coloredIdx, 0.0, -1, -1);
                    fwdPQ.insert(coloredIdx, 0.0);
                }
            }
        }

        double bestCost   = Double.POSITIVE_INFINITY;
        int    meetingNode = -1;

        while (!fwdPQ.isEmpty() || !bwdPQ.isEmpty()) {
            // Stopping criterion: no path cheaper than bestCost can still be discovered.
            double fMin = fwdPQ.isEmpty() ? Double.POSITIVE_INFINITY : fwdCost[fwdPQ.peek()];
            double bMin = bwdPQ.isEmpty() ? Double.POSITIVE_INFINITY : bwdCost[bwdPQ.peek()];
            if (fMin + bMin >= bestCost) break;

            // Choose the side with the smaller tentative minimum.
            boolean expandForward = !fwdPQ.isEmpty()
                    && (bwdPQ.isEmpty() || fMin <= bMin);

            if (expandForward) {
                int v    = fwdPQ.poll();
                double d = fwdCost[v];

                // Check meeting point.
                if (bwdIterIds[v] == currentIteration) {
                    double total = d + bwdCost[v];
                    if (total < bestCost) {
                        bestCost    = total;
                        meetingNode = v;
                    }
                }

                // Relax upward out-edges.
                outUpIter.reset(v);
                while (outUpIter.next()) {
                    int    e       = outUpIter.getEdgeIndex();
                    int    w       = outUpIter.getToNode();
                    double newCost = d + chGraph.edgeWeights[e];
                    if (fwdIterIds[w] == currentIteration) {
                        if (newCost < fwdCost[w]) {
                            fwdCost[w] = newCost;
                            fwdComingFrom[w] = v;
                            fwdUsedEdge[w]   = e;
                            fwdPQ.decreaseKey(w, newCost);
                        } else if (newCost == fwdCost[w] && e < fwdUsedEdge[w]) {
                            // Tie-breaking: prefer lower edge index for deterministic paths.
                            fwdComingFrom[w] = v;
                            fwdUsedEdge[w]   = e;
                        }
                    } else {
                        setFwd(w, newCost, v, e);
                        fwdPQ.insert(w, newCost);
                    }
                }

            } else {
                int v    = bwdPQ.poll();
                double d = bwdCost[v];

                // Check meeting point.
                if (fwdIterIds[v] == currentIteration) {
                    double total = fwdCost[v] + d;
                    if (total < bestCost) {
                        bestCost    = total;
                        meetingNode = v;
                    }
                }

                // Relax downward in-edges (backward search goes from y via edge y→v, so
                // we update y's backward cost as bwdCost[y] = bwdCost[v] + w(y→v)).
                downInIter.reset(v);
                while (downInIter.next()) {
                    int    e       = downInIter.getEdgeIndex();
                    int    y       = downInIter.getFromNode(); // higher-level node
                    double newCost = d + chGraph.edgeWeights[e];
                    if (bwdIterIds[y] == currentIteration) {
                        if (newCost < bwdCost[y]) {
                            bwdCost[y] = newCost;
                            bwdComingFrom[y] = v;
                            bwdUsedEdge[y]   = e;
                            bwdPQ.decreaseKey(y, newCost);
                        } else if (newCost == bwdCost[y] && e < bwdUsedEdge[y]) {
                            // Tie-breaking: prefer lower edge index for deterministic paths.
                            bwdComingFrom[y] = v;
                            bwdUsedEdge[y]   = e;
                        }
                    } else {
                        setBwd(y, newCost, v, e);
                        bwdPQ.insert(y, newCost);
                    }
                }
            }
        }

        if (meetingNode < 0) return null;
        return constructPath(startIdx, meetingNode, startTime, person, vehicle);
    }

    // -------------------------------------------------------------------------
    // Path construction and shortcut unpacking
    // -------------------------------------------------------------------------

    private Path constructPath(int startIdx, int meetingNode,
                               double startTime, Person person, Vehicle vehicle) {
        List<Node> nodeList = new ArrayList<>();
        List<Link> linkList = new ArrayList<>();

        nodeList.add(baseGraph.getNode(startIdx));

        // --- Forward part: collect CH edges from meeting node back to source, then reverse ---
        List<Integer> fwdEdges = new ArrayList<>();
        int curr = meetingNode;
        while (fwdComingFrom[curr] >= 0) {
            fwdEdges.add(fwdUsedEdge[curr]);
            curr = fwdComingFrom[curr];
        }
        Collections.reverse(fwdEdges);
        for (int edgeIdx : fwdEdges) {
            unpackEdge(edgeIdx, nodeList, linkList);
        }
        // meetingNode is now the last entry in nodeList (or startIdx if no forward edges).

        // --- Backward part: follow bwdComingFrom chain from meeting node to target ---
        curr = meetingNode;
        while (bwdComingFrom[curr] >= 0) {
            unpackEdge(bwdUsedEdge[curr], nodeList, linkList);
            curr = bwdComingFrom[curr];
        }

        // Compute actual travel time and cost by replaying the unpacked links.
        double time = startTime;
        double cost = 0.0;
        for (Link link : linkList) {
            double travelTime = tt.getLinkTravelTime(link, time, person, vehicle);
            cost += td.getLinkTravelDisutility(link, time, person, vehicle);
            time += travelTime;
        }

        return new Path(nodeList, linkList, time - startTime, cost);
    }

    /**
     * Recursively unpacks a CH edge, appending intermediate nodes (exclusive of fromNode,
     * inclusive of toNode) and original links to the provided lists.
     */
    private void unpackEdge(int edgeIdx, List<Node> nodeList, List<Link> linkList) {
        int base     = edgeIdx * SpeedyCHGraph.EDGE_SIZE;
        int origLink = chGraph.edgeData[base + 4];
        int toNode   = chGraph.edgeData[base + 3];

        if (origLink >= 0) {
            // Real edge.
            linkList.add(baseGraph.getLink(origLink));
            nodeList.add(baseGraph.getNode(toNode));
        } else {
            // Shortcut: unpack via middleNode.
            int lower1 = chGraph.edgeData[base + 6];
            int lower2 = chGraph.edgeData[base + 7];
            // lower1 = edge from→middle (adds nodes/links up to middle).
            // lower2 = edge middle→to   (adds nodes/links up to toNode).
            unpackEdge(lower1, nodeList, linkList);
            unpackEdge(lower2, nodeList, linkList);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void advanceIteration() {
        currentIteration++;
        if (currentIteration == Integer.MAX_VALUE) {
            Arrays.fill(fwdIterIds, Integer.MIN_VALUE);
            Arrays.fill(bwdIterIds, Integer.MIN_VALUE);
            currentIteration = Integer.MIN_VALUE + 1;
        }
    }

    private void setFwd(int node, double cost, int comingFrom, int usedEdge) {
        fwdCost[node]       = cost;
        fwdComingFrom[node] = comingFrom;
        fwdUsedEdge[node]   = usedEdge;
        fwdIterIds[node]    = currentIteration;
    }

    private void setBwd(int node, double cost, int comingFrom, int usedEdge) {
        bwdCost[node]       = cost;
        bwdComingFrom[node] = comingFrom;
        bwdUsedEdge[node]   = usedEdge;
        bwdIterIds[node]    = currentIteration;
    }

    private static void logNoRoute(String from, String to) {
        LOG.warn("No route was found from {} to {}. Some possible reasons:", from, to);
        LOG.warn("  * Network is not connected.  Run NetworkUtils.cleanNetwork(...).");
        LOG.warn("  * Network for considered mode does not even exist.");
        LOG.warn("  * Network for considered mode is not connected to start/end point.");
        LOG.warn("This will now return null, but it may fail later with a NullPointerException.");
    }
}
