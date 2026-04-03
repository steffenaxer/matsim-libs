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

/**
 * Time-dependent CATCHUp bidirectional CH query.
 *
 * <p>Requires a {@link SpeedyCHGraph} whose {@link SpeedyCHTTFCustomizer} has already been
 * applied (i.e., {@link SpeedyCHGraph#ttf} and {@link SpeedyCHGraph#minTTF} are non-null).
 *
 * <h3>Algorithm</h3>
 * <ul>
 *   <li><b>Forward search</b> from source: evaluates {@code ttf[e][ bin(currentTime) ]} at each
 *       relaxed edge; tracks <em>absolute arrival time</em> at every node.</li>
 *   <li><b>Backward search</b> from target: uses {@code minTTF[e]} (the per-edge minimum
 *       travel time over all time bins) as a conservative FIFO lower bound.</li>
 *   <li><b>Meeting node</b>: the node that minimises
 *       {@code fwdArrival[v] + bwdLowerBound[v]}.</li>
 *   <li><b>Stopping criterion</b>: {@code fwdPQ.minArrival + bwdPQ.minLB >= bestBound}.</li>
 *   <li><b>Path reconstruction</b>: shortcuts are unpacked recursively; actual travel times
 *       and disutilities are recomputed by a forward pass over the original links.</li>
 * </ul>
 *
 * <p>This implementation is <b>not thread-safe</b>. Every thread must own a separate instance;
 * the underlying {@link SpeedyCHGraph} may be shared.
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHTimeDep implements LeastCostPathCalculator {

    private static final Logger LOG = LogManager.getLogger(SpeedyCHTimeDep.class);

    private final SpeedyCHGraph chGraph;
    private final SpeedyGraph   baseGraph;
    private final TravelTime       tt;
    private final TravelDisutility td;

    // Forward search: tracks absolute arrival times.
    private final double[] fwdArrival;   // absolute arrival time at node
    private final int[]    fwdComingFrom;
    private final int[]    fwdUsedEdge;
    private final int[]    fwdIterIds;

    // Backward search: tracks lower-bound remaining travel times.
    private final double[] bwdLB;        // lower-bound remaining time to target
    private final int[]    bwdComingFrom;
    private final int[]    bwdUsedEdge;
    private final int[]    bwdIterIds;

    private int currentIteration = Integer.MIN_VALUE;

    private final DAryMinHeap fwdPQ;
    private final DAryMinHeap bwdPQ;

    private final SpeedyCHGraph.UpwardOutEdgeIterator  outUpIter;
    private final SpeedyCHGraph.DownwardInEdgeIterator downInIter;

    public SpeedyCHTimeDep(SpeedyCHGraph chGraph, TravelTime tt, TravelDisutility td) {
        if (chGraph.ttf == null || chGraph.minTTF == null) {
            throw new IllegalArgumentException(
                    "SpeedyCHGraph has no TTF data. Run SpeedyCHTTFCustomizer first.");
        }
        this.chGraph   = chGraph;
        this.baseGraph = chGraph.getBaseGraph();
        this.tt        = tt;
        this.td        = td;

        int n = chGraph.nodeCount;
        this.fwdArrival    = new double[n];
        this.fwdComingFrom = new int[n];
        this.fwdUsedEdge   = new int[n];
        this.fwdIterIds    = new int[n];

        this.bwdLB         = new double[n];
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
    // Bidirectional CATCHUp query
    // -------------------------------------------------------------------------

    private Path calcLeastCostPathImpl(int startIdx, int endIdx,
                                       double startTime, Person person, Vehicle vehicle) {
        advanceIteration();

        // Trivial same-node case.
        if (startIdx == endIdx) {
            return new Path(
                    Collections.singletonList(baseGraph.getNode(startIdx)),
                    Collections.emptyList(), 0.0, 0.0);
        }

        // Initialize forward search (absolute arrival times).
        setFwd(startIdx, startTime, -1, -1);
        fwdPQ.clear();
        fwdPQ.insert(startIdx, startTime);

        // Initialize backward search (lower-bound remaining times).
        setBwd(endIdx, 0.0, -1, -1);
        bwdPQ.clear();
        bwdPQ.insert(endIdx, 0.0);

        double bestBound   = Double.POSITIVE_INFINITY;
        int    meetingNode = -1;

        while (!fwdPQ.isEmpty() || !bwdPQ.isEmpty()) {
            // Stopping criterion: fwdMin + bwdMin >= best proven lower bound.
            double fMin = fwdPQ.isEmpty() ? Double.POSITIVE_INFINITY : fwdArrival[fwdPQ.peek()];
            double bMin = bwdPQ.isEmpty() ? Double.POSITIVE_INFINITY : bwdLB[bwdPQ.peek()];
            if (fMin + bMin >= bestBound) break;

            // Expand the side with the smaller key.
            boolean expandForward = !fwdPQ.isEmpty()
                    && (bwdPQ.isEmpty() || fMin <= bMin);

            if (expandForward) {
                int v      = fwdPQ.poll();
                double arr = fwdArrival[v];

                // Update meeting point.
                if (bwdIterIds[v] == currentIteration) {
                    double bound = arr + bwdLB[v];
                    if (bound < bestBound) {
                        bestBound   = bound;
                        meetingNode = v;
                    }
                }

                // Relax upward out-edges (time-dependent TTF lookup).
                int bin = SpeedyCHTTFCustomizer.timeToBin(arr);
                outUpIter.reset(v);
                while (outUpIter.next()) {
                    int    e          = outUpIter.getEdgeIndex();
                    int    w          = outUpIter.getToNode();
                    double travelTime = chGraph.ttf[e][bin];
                    double newArr     = arr + travelTime;

                    if (fwdIterIds[w] == currentIteration) {
                        if (newArr < fwdArrival[w]) {
                            fwdArrival[w]    = newArr;
                            fwdComingFrom[w] = v;
                            fwdUsedEdge[w]   = e;
                            fwdPQ.decreaseKey(w, newArr);
                        }
                    } else {
                        setFwd(w, newArr, v, e);
                        fwdPQ.insert(w, newArr);
                    }
                }

            } else {
                int v   = bwdPQ.poll();
                double lb = bwdLB[v];

                // Update meeting point.
                if (fwdIterIds[v] == currentIteration) {
                    double bound = fwdArrival[v] + lb;
                    if (bound < bestBound) {
                        bestBound   = bound;
                        meetingNode = v;
                    }
                }

                // Relax downward in-edges using minTTF lower bounds.
                downInIter.reset(v);
                while (downInIter.next()) {
                    int    e      = downInIter.getEdgeIndex();
                    int    y      = downInIter.getFromNode(); // higher-level predecessor
                    double newLB  = lb + chGraph.minTTF[e];

                    if (bwdIterIds[y] == currentIteration) {
                        if (newLB < bwdLB[y]) {
                            bwdLB[y]         = newLB;
                            bwdComingFrom[y] = v;
                            bwdUsedEdge[y]   = e;
                            bwdPQ.decreaseKey(y, newLB);
                        }
                    } else {
                        setBwd(y, newLB, v, e);
                        bwdPQ.insert(y, newLB);
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

        // --- Collect forward CH edges (source → meeting node) ---
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

        // --- Follow backward CH edges (meeting node → target) ---
        curr = meetingNode;
        while (bwdComingFrom[curr] >= 0) {
            unpackEdge(bwdUsedEdge[curr], nodeList, linkList);
            curr = bwdComingFrom[curr];
        }

        // --- Compute actual travel time and disutility over the unpacked path ---
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
     * Recursively unpacks a CH edge: appends intermediate nodes (exclusive of
     * fromNode, inclusive of toNode) and original links to the provided lists.
     */
    private void unpackEdge(int edgeIdx, List<Node> nodeList, List<Link> linkList) {
        int base     = edgeIdx * SpeedyCHGraph.EDGE_SIZE;
        int origLink = chGraph.edgeData[base + 4];
        int toNode   = chGraph.edgeData[base + 3];

        if (origLink >= 0) {
            linkList.add(baseGraph.getLink(origLink));
            nodeList.add(baseGraph.getNode(toNode));
        } else {
            // Shortcut: recursively unpack sub-edges via middleNode.
            int lower1 = chGraph.edgeData[base + 6];
            int lower2 = chGraph.edgeData[base + 7];
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

    private void setFwd(int node, double arrival, int comingFrom, int usedEdge) {
        fwdArrival[node]    = arrival;
        fwdComingFrom[node] = comingFrom;
        fwdUsedEdge[node]   = usedEdge;
        fwdIterIds[node]    = currentIteration;
    }

    private void setBwd(int node, double lb, int comingFrom, int usedEdge) {
        bwdLB[node]         = lb;
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
