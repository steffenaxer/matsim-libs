package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Node;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;
import org.matsim.core.utils.misc.OptionalTime;
import org.matsim.vehicles.Vehicle;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * CH-based least-cost-path tree.  Computes shortest paths from one source
 * to ALL reachable nodes using the CH overlay graph, which is dramatically
 * faster than plain Dijkstra on the full base graph.
 *
 * <h3>Algorithm (forward search)</h3>
 * <ol>
 *   <li><b>Phase 1 – Upward Dijkstra</b>: From the source, explore only upward
 *       CH edges (towards higher-ranked nodes).  This settles all nodes
 *       reachable via upward paths in the CH overlay.</li>
 *   <li><b>Phase 2 – Downward sweep</b>: Process all nodes in <em>decreasing</em>
 *       rank order.  For each node w, check its incoming downward edges (from
 *       higher-ranked nodes u).  If {@code cost[u] + weight(u→w) < cost[w]},
 *       update w.  After the sweep, every node has its optimal shortest path
 *       cost.</li>
 * </ol>
 *
 * <h3>Algorithm (backward search)</h3>
 * <ol>
 *   <li><b>Phase 1 – "Reverse upward" Dijkstra</b>: From the target, explore
 *       downward in-edges in reverse.  For each node w being processed, its
 *       {@code dnEdges} give edges from higher-ranked u to w; going backward
 *       settles u with cost {@code cost[w] + weight(u→w)}.</li>
 *   <li><b>Phase 2 – Downward sweep</b>: Process all nodes in decreasing rank
 *       order.  For each node w, check its upward out-edges (to higher-ranked u).
 *       Going backward: {@code cost[w] = min(cost[w], cost[u] + weight(w→u))}.</li>
 * </ol>
 *
 * <p>This class implements the same interface pattern as {@link LeastCostPathTree}
 * so it can be used as a drop-in replacement in {@code OneToManyPathSearch}.
 *
 * <p>The implementation does not allocate any memory in the calculate methods.
 * All required memory is pre-allocated in the constructor. This makes the
 * implementation NOT thread-safe.
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHLeastCostPathTree implements ShortestPathTree {

    private final SpeedyCHGraph chGraph;
    private final SpeedyGraph baseGraph;
    private final TravelTime tt;
    private final TravelDisutility td;

    // Per-node data: 3 entries (cost, time, distance)
    private final double[] data;
    private final int[] comingFrom;       // parent node index in the tree
    private final int[] fromEdgeGIdx;     // CH global edge index used to reach this node
    private final int[] iterIds;          // iteration stamp to detect unvisited nodes

    private int currentIteration = Integer.MIN_VALUE;

    private final DAryMinHeap pq;

    // Cached CH arrays for hot-path access
    private final int[] upOff, upLen, upEdges;
    private final double[] upWeights;
    private final int[] dnOff, dnLen, dnEdges;
    private final double[] dnWeights;
    private final int[] sweepOrder;
    private final double[] ttf;
    private final int totalEdgeCount;

    // Reverse CSR arrays for push-based Phase 2 sweep
    private final int[] dnOutOff, dnOutLen, dnOutEdges;
    private final int[] upInOff, upInLen, upInEdges;
    private final int[] nodeLevel;
    private final int nodeCount;

    // Phase 1 settled-node tracking (for lazy Phase 2)
    private final int[] settledNodes;
    private int settledCount;

    // Sweep iteration counter (to detect nodes in sweep PQ without per-query O(N) reset)
    private final int[] sweepIds;
    private int sweepIteration = Integer.MIN_VALUE;

    public SpeedyCHLeastCostPathTree(SpeedyCHGraph chGraph, TravelTime tt, TravelDisutility td) {
        this.chGraph = chGraph;
        this.baseGraph = chGraph.getBaseGraph();
        this.tt = tt;
        this.td = td;

        int n = chGraph.nodeCount;
        this.data = new double[n * 3];
        this.comingFrom = new int[n];
        this.fromEdgeGIdx = new int[n];
        this.iterIds = new int[n];
        this.pq = new DAryMinHeap(n, 4);

        this.upOff = chGraph.upOff;
        this.upLen = chGraph.upLen;
        this.upEdges = chGraph.upEdges;
        this.upWeights = chGraph.upWeights;
        this.dnOff = chGraph.dnOff;
        this.dnLen = chGraph.dnLen;
        this.dnEdges = chGraph.dnEdges;
        this.dnWeights = chGraph.dnWeights;
        this.sweepOrder = chGraph.sweepOrder;
        this.ttf = chGraph.ttf;
        this.totalEdgeCount = chGraph.totalEdgeCount;

        // Reverse CSR for push-based Phase 2
        this.dnOutOff = chGraph.dnOutOff;
        this.dnOutLen = chGraph.dnOutLen;
        this.dnOutEdges = chGraph.dnOutEdges;
        this.upInOff = chGraph.upInOff;
        this.upInLen = chGraph.upInLen;
        this.upInEdges = chGraph.upInEdges;
        this.nodeLevel = chGraph.nodeLevel;
        this.nodeCount = n;

        // Phase 2 optimization state
        this.settledNodes = new int[n];
        this.sweepIds = new int[n];
    }

    // -------------------------------------------------------------------------
    // Forward search
    // -------------------------------------------------------------------------

    public void calculate(Link startLink, double startTime, Person person, Vehicle vehicle) {
        calculate(startLink, startTime, person, vehicle,
                (node, arrTime, cost, distance, depTime) -> false);
    }

    public void calculate(Link startLink, double startTime, Person person, Vehicle vehicle,
                          LeastCostPathTree.StopCriterion stopCriterion) {
        int startNode = startLink.getToNode().getId().index();
        calculateForward(startNode, startTime, stopCriterion);
    }

    private void calculateForward(int startNode, double startTime,
                                  LeastCostPathTree.StopCriterion stopCriterion) {
        advanceIteration();

        final int S = SpeedyCHGraph.E_STRIDE;
        final int NUM_BINS = SpeedyCHTTFCustomizer.NUM_BINS;
        final double INV_BIN = SpeedyCHTTFCustomizer.INV_BIN_SIZE;

        // Phase 1: Upward Dijkstra from source
        // Uses time-dependent TTF for travel time; cost = travel time (consistent
        // with SpeedyCHTimeDep which also uses TTF as the cost metric).
        setNode(startNode, 0.0, startTime, 0.0, -1, -1);
        pq.clear();
        pq.insert(startNode, 0.0);
        settledCount = 0;

        while (!pq.isEmpty()) {
            int v = pq.poll();
            settledNodes[settledCount++] = v;
            double cost = getCost(v);
            double arr = getTimeRaw(v);

            // Early termination: safe because nodes are polled in increasing
            // cost order (standard Dijkstra).  Remaining PQ nodes have cost ≥
            // this node's cost, so any path through them cannot improve targets
            // with cost ≤ this node's cost.  Phase 2 still runs unconditionally
            // to finalize costs from already-settled nodes.
            if (stopCriterion.stop(baseGraph.getNode(v).getId().index(),
                    arr, cost, getDistance(v), startTime)) {
                break;
            }

            // Compute time bin for TTF lookup
            int bin = ((int) (arr * INV_BIN)) % NUM_BINS;
            if (bin < 0) bin += NUM_BINS;
            int binOff = bin * totalEdgeCount;

            // Iterate upward out-edges
            int uOff = upOff[v];
            int uEnd = uOff + upLen[v];
            for (int slot = uOff; slot < uEnd; slot++) {
                int eBase = slot * S;
                int w = upEdges[eBase];
                int gIdx = upEdges[eBase + SpeedyCHGraph.E_GIDX];

                double tTime = ttf[binOff + gIdx];
                double newCost = cost + tTime;
                double newArr = arr + tTime;

                if (iterIds[w] == currentIteration) {
                    if (newCost < getCost(w)) {
                        setNode(w, newCost, newArr, 0.0, v, gIdx);
                        pq.decreaseKey(w, newCost);
                    }
                } else {
                    setNode(w, newCost, newArr, 0.0, v, gIdx);
                    pq.insert(w, newCost);
                }
            }
        }

        // Phase 2: Downward propagation from settled nodes.
        // If Phase 1 drained the PQ completely (no early termination), the full
        // linear sweep is faster (O(N+E) without heap overhead).  If Phase 1
        // terminated early (PQ not empty), only settled nodes and their transitive
        // downward successors need processing — use a heap-based push approach
        // that is proportional to the reachable set, not the total graph.
        boolean earlyTerminated = !pq.isEmpty();
        if (earlyTerminated) {
            forwardSweepLazy(S, NUM_BINS, INV_BIN);
        } else {
            forwardSweepFull(S, NUM_BINS, INV_BIN);
        }
    }

    /**
     * Full downward sweep: processes ALL nodes in decreasing rank order.
     * Used when Phase 1 explored the entire reachable graph.
     */
    private void forwardSweepFull(int S, int NUM_BINS, double INV_BIN) {
        for (int i = 0; i < sweepOrder.length; i++) {
            int w = sweepOrder[i];

            int dOff = dnOff[w];
            int dEnd = dOff + dnLen[w];
            for (int slot = dOff; slot < dEnd; slot++) {
                int eBase = slot * S;
                int u = dnEdges[eBase];
                int gIdx = dnEdges[eBase + SpeedyCHGraph.E_GIDX];

                if (iterIds[u] != currentIteration) continue;

                double uCost = getCost(u);
                double uArr = getTimeRaw(u);

                int bin = ((int) (uArr * INV_BIN)) % NUM_BINS;
                if (bin < 0) bin += NUM_BINS;
                double tTime = ttf[bin * totalEdgeCount + gIdx];

                double newCost = uCost + tTime;
                double newArr = uArr + tTime;

                double wCost = (iterIds[w] == currentIteration)
                        ? getCost(w) : Double.POSITIVE_INFINITY;

                if (newCost < wCost) {
                    setNode(w, newCost, newArr, 0.0, u, gIdx);
                }
            }
        }
    }

    /**
     * Lazy downward sweep: pushes costs from settled nodes to lower-ranked
     * successors via the reverse CSR (dnOutEdges).  Only processes nodes
     * reachable from Phase 1 settled nodes — proportional to the reachable
     * set rather than the total graph.  Ordering by decreasing rank ensures
     * that when a node is polled, all its higher-ranked predecessors have
     * been finalized.
     */
    private void forwardSweepLazy(int S, int NUM_BINS, double INV_BIN) {
        advanceSweepIteration();
        pq.clear();

        // Seed the sweep heap with all Phase 1 settled nodes.
        // Key = nodeCount - nodeLevel[node]: min-heap delivers highest rank first.
        for (int i = 0; i < settledCount; i++) {
            int node = settledNodes[i];
            pq.insert(node, nodeCount - nodeLevel[node]);
            sweepIds[node] = sweepIteration;
        }

        while (!pq.isEmpty()) {
            int u = pq.poll();

            double uCost = getCost(u);
            double uArr = getTimeRaw(u);

            // Compute time bin for u's departure
            int bin = ((int) (uArr * INV_BIN)) % NUM_BINS;
            if (bin < 0) bin += NUM_BINS;
            int binOff = bin * totalEdgeCount;

            // Push costs along u's outgoing downward edges (u→w, rank(w) < rank(u))
            int dOff = dnOutOff[u];
            int dEnd = dOff + dnOutLen[u];
            for (int slot = dOff; slot < dEnd; slot++) {
                int eBase = slot * S;
                int w = dnOutEdges[eBase];
                int gIdx = dnOutEdges[eBase + SpeedyCHGraph.E_GIDX];

                double tTime = ttf[binOff + gIdx];
                double newCost = uCost + tTime;
                double newArr = uArr + tTime;

                double wCost = (iterIds[w] == currentIteration)
                        ? getCost(w) : Double.POSITIVE_INFINITY;

                if (newCost < wCost) {
                    setNode(w, newCost, newArr, 0.0, u, gIdx);

                    // Add w to sweep heap if not already there
                    if (sweepIds[w] != sweepIteration) {
                        sweepIds[w] = sweepIteration;
                        pq.insert(w, nodeCount - nodeLevel[w]);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Backward search
    // -------------------------------------------------------------------------

    public void calculateBackwards(Link arrivalLink, double arrivalTime, Person person, Vehicle vehicle) {
        calculateBackwards(arrivalLink, arrivalTime, person, vehicle,
                (node, arrTime, cost, distance, depTime) -> false);
    }

    public void calculateBackwards(Link arrivalLink, double arrivalTime, Person person, Vehicle vehicle,
                                   LeastCostPathTree.StopCriterion stopCriterion) {
        int arrivalNode = arrivalLink.getFromNode().getId().index();
        calculateBackwardImpl(arrivalNode, arrivalTime, stopCriterion);
    }

    private void calculateBackwardImpl(int targetNode, double arrivalTime,
                                        LeastCostPathTree.StopCriterion stopCriterion) {
        advanceIteration();

        final int S = SpeedyCHGraph.E_STRIDE;

        // Phase 1: Backward "upward" Dijkstra from target.
        setNode(targetNode, 0.0, arrivalTime, 0.0, -1, -1);
        pq.clear();
        pq.insert(targetNode, 0.0);
        settledCount = 0;

        while (!pq.isEmpty()) {
            int w = pq.poll();
            settledNodes[settledCount++] = w;
            double cost = getCost(w);

            // Early termination: safe because backward Dijkstra also polls
            // in increasing cost order.  See calculateForward for rationale.
            if (stopCriterion.stop(baseGraph.getNode(w).getId().index(),
                    arrivalTime, cost, getDistance(w), getTimeRaw(w))) {
                break;
            }

            int dOff = dnOff[w];
            int dEnd = dOff + dnLen[w];
            for (int slot = dOff; slot < dEnd; slot++) {
                int eBase = slot * S;
                int u = dnEdges[eBase];
                int gIdx = dnEdges[eBase + SpeedyCHGraph.E_GIDX];

                double edgeCost = chGraph.minTTF[gIdx];
                double newCost = cost + edgeCost;

                if (iterIds[u] == currentIteration) {
                    if (newCost < getCost(u)) {
                        double newTime = getTimeRaw(w) - edgeCost;
                        setNode(u, newCost, newTime, 0.0, w, gIdx);
                        pq.decreaseKey(u, newCost);
                    }
                } else {
                    double newTime = getTimeRaw(w) - edgeCost;
                    setNode(u, newCost, newTime, 0.0, w, gIdx);
                    pq.insert(u, newCost);
                }
            }
        }

        // Phase 2: Downward propagation.
        // Same strategy as forward: use lazy sweep when Phase 1 terminated early.
        boolean earlyTerminated = !pq.isEmpty();
        if (earlyTerminated) {
            backwardSweepLazy(S);
        } else {
            backwardSweepFull(S);
        }
    }

    /**
     * Full backward downward sweep: processes ALL nodes in decreasing rank order.
     */
    private void backwardSweepFull(int S) {
        for (int i = 0; i < sweepOrder.length; i++) {
            int w = sweepOrder[i];

            int uOff = upOff[w];
            int uEnd = uOff + upLen[w];

            double wCost = (iterIds[w] == currentIteration) ? getCost(w) : Double.POSITIVE_INFINITY;

            for (int slot = uOff; slot < uEnd; slot++) {
                int eBase = slot * S;
                int u = upEdges[eBase]; // higher-ranked toNode
                int gIdx = upEdges[eBase + SpeedyCHGraph.E_GIDX];

                if (iterIds[u] != currentIteration) continue;

                double edgeCost = chGraph.minTTF[gIdx];
                double newCost = getCost(u) + edgeCost;

                if (newCost < wCost) {
                    wCost = newCost;
                    double newTime = getTimeRaw(u) - edgeCost;
                    setNode(w, newCost, newTime, 0.0, u, gIdx);
                }
            }
        }
    }

    /**
     * Lazy backward downward sweep: pushes costs from settled nodes to
     * lower-ranked predecessors via the reverse CSR (upInEdges).
     */
    private void backwardSweepLazy(int S) {
        advanceSweepIteration();
        pq.clear();

        for (int i = 0; i < settledCount; i++) {
            int node = settledNodes[i];
            pq.insert(node, nodeCount - nodeLevel[node]);
            sweepIds[node] = sweepIteration;
        }

        while (!pq.isEmpty()) {
            int u = pq.poll();

            // Push cost from u back to lower-ranked w via incoming up-edges (w->u).
            int iOff = upInOff[u];
            int iEnd = iOff + upInLen[u];
            for (int slot = iOff; slot < iEnd; slot++) {
                int eBase = slot * S;
                int w = upInEdges[eBase];       // lower-ranked source
                int gIdx = upInEdges[eBase + SpeedyCHGraph.E_GIDX];

                double edgeCost = chGraph.minTTF[gIdx];
                double newCost = getCost(u) + edgeCost;

                double wCost = (iterIds[w] == currentIteration) ? getCost(w) : Double.POSITIVE_INFINITY;

                if (newCost < wCost) {
                    double newTime = getTimeRaw(u) - edgeCost;
                    setNode(w, newCost, newTime, 0.0, u, gIdx);

                    if (sweepIds[w] != sweepIteration) {
                        sweepIds[w] = sweepIteration;
                        pq.insert(w, nodeCount - nodeLevel[w]);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Accessors (compatible with LeastCostPathTree interface)
    // -------------------------------------------------------------------------

    public double getCost(int nodeIndex) {
        return data[nodeIndex * 3];
    }

    private double getTimeRaw(int nodeIndex) {
        return data[nodeIndex * 3 + 1];
    }

    public OptionalTime getTime(int nodeIndex) {
        if (iterIds[nodeIndex] != currentIteration) return OptionalTime.undefined();
        double time = getTimeRaw(nodeIndex);
        if (Double.isInfinite(time)) return OptionalTime.undefined();
        return OptionalTime.defined(time);
    }

    public double getDistance(int nodeIndex) {
        return data[nodeIndex * 3 + 2];
    }

    /**
     * Returns a node-path iterator that walks from the given target node
     * back to the source through the CH tree.
     */
    @Override
    public Iterator<Node> getNodePathIterator(Node node) {
        return new CHPathIterator(node);
    }

    /**
     * Returns a link-path iterator that unpacks CH shortcuts into base-graph
     * links, walking from the given target node back to the source.
     */
    @Override
    public Iterator<Link> getLinkPathIterator(Node node) {
        return new CHLinkPathIterator(node);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void advanceIteration() {
        currentIteration++;
        if (currentIteration == Integer.MAX_VALUE) {
            Arrays.fill(iterIds, Integer.MIN_VALUE);
            currentIteration = Integer.MIN_VALUE + 1;
        }
    }

    private void advanceSweepIteration() {
        sweepIteration++;
        if (sweepIteration == Integer.MAX_VALUE) {
            Arrays.fill(sweepIds, Integer.MIN_VALUE);
            sweepIteration = Integer.MIN_VALUE + 1;
        }
    }

    private void setNode(int nodeIndex, double cost, double time, double distance,
                         int from, int edgeGIdx) {
        int base = nodeIndex * 3;
        data[base] = cost;
        data[base + 1] = time;
        data[base + 2] = distance;
        comingFrom[nodeIndex] = from;
        fromEdgeGIdx[nodeIndex] = edgeGIdx;
        iterIds[nodeIndex] = currentIteration;
    }

    // -------------------------------------------------------------------------
    // Path iterators (unpack CH shortcuts to base-graph nodes/links)
    // -------------------------------------------------------------------------

    /**
     * Iterates through the CH parent chain, yielding base-graph nodes by
     * unpacking shortcuts along the way. Compatible with
     * {@link LeastCostPathTree.PathIterator}.
     */
    private final class CHPathIterator implements Iterator<Node> {
        private int current;

        CHPathIterator(Node startNode) {
            current = startNode.getId().index();
        }

        @Override
        public Node next() {
            current = comingFrom[current];
            if (current < 0) throw new NoSuchElementException();
            return baseGraph.getNode(current);
        }

        @Override
        public boolean hasNext() {
            return current >= 0 && comingFrom[current] >= 0;
        }
    }

    /**
     * Iterates through CH edges, unpacking shortcuts into base-graph links.
     * Compatible with {@link LeastCostPathTree.LinkPathIterator}.
     */
    private final class CHLinkPathIterator implements Iterator<Link> {
        // We build the full unpacked link list eagerly because shortcuts
        // expand into multiple links and the iterator interface is sequential.
        private final Link[] links;
        private int pos;

        CHLinkPathIterator(Node targetNode) {
            int target = targetNode.getId().index();
            // Collect all CH edges on the path (from target to source)
            java.util.List<Integer> edgeGIdxList = new java.util.ArrayList<>();
            int curr = target;
            while (curr >= 0 && iterIds[curr] == currentIteration && comingFrom[curr] >= 0) {
                edgeGIdxList.add(fromEdgeGIdx[curr]);
                curr = comingFrom[curr];
            }
            // Unpack all CH edges to base-graph links
            java.util.List<Link> linkList = new java.util.ArrayList<>();
            for (int gIdx : edgeGIdxList) {
                unpackEdge(gIdx, linkList);
            }
            links = linkList.toArray(new Link[0]);
            pos = 0;
        }

        private void unpackEdge(int gIdx, java.util.List<Link> linkList) {
            if (gIdx < 0) return;
            int orig = chGraph.edgeOrigLink[gIdx];
            if (orig >= 0) {
                linkList.add(baseGraph.getLink(orig));
            } else {
                unpackEdge(chGraph.edgeLower1[gIdx], linkList);
                unpackEdge(chGraph.edgeLower2[gIdx], linkList);
            }
        }

        @Override
        public Link next() {
            if (pos >= links.length) throw new NoSuchElementException();
            return links[pos++];
        }

        @Override
        public boolean hasNext() {
            return pos < links.length;
        }
    }
}
