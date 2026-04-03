package org.matsim.core.router.speedy;

import org.matsim.core.router.util.TravelDisutility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Builds a {@link SpeedyCHGraph} from a {@link SpeedyGraph} using the
 * Contraction Hierarchies (CH) algorithm.
 *
 * <p>Node ordering uses the <em>edge-difference</em> heuristic with lazy priority update:
 * <pre>priority(v) = #shortcuts_added − #edges_removed + #contracted_neighbors</pre>
 *
 * <p>A local Dijkstra <em>witness search</em> (cost-bounded, hop-limit = {@value #HOP_LIMIT})
 * determines whether a shortcut is needed between any pair of uncontracted neighbors.
 *
 * <p>After all nodes are contracted, the final {@link SpeedyCHGraph} is constructed by
 * separating edges into upward (for forward search) and downward (for backward search).
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHBuilder {

    // Build-edge field indices (parallel arrays)
    private static final int BE_FROM  = 0;
    private static final int BE_TO    = 1;
    private static final int BE_ORIG  = 2; // originalLinkIndex; -1 = shortcut
    private static final int BE_MID   = 3; // middleNode;        -1 = real edge
    private static final int BE_LOW1  = 4; // lowerEdge1 build index; -1 = real edge
    private static final int BE_LOW2  = 5; // lowerEdge2 build index; -1 = real edge
    private static final int BE_SIZE  = 6;

    /** Maximum hops allowed in the witness search. */
    private static final int HOP_LIMIT = 5;

    private final SpeedyGraph graph;
    private final TravelDisutility td;
    private final int nodeCount;

    // Build-time edge storage (grows dynamically)
    private int   buildEdgeCount = 0;
    private int[] buildEdgeData;
    private double[] buildEdgeWeights;

    // Per-node adjacency (build time, mutable)
    private final List<Integer>[] outEdgeList;
    private final List<Integer>[] inEdgeList;

    // CH state
    private final int[]     nodeLevel;
    private final boolean[] contracted;
    private final int[]     contractedNeighborCount;

    // Witness-search reusable state
    private int    witnessIteration = 0;
    private final int[]    witnessIterIds;
    private final double[] witnessCost;
    // Reusable PQ for witness search (cleared via .clear() between searches)
    private final PriorityQueue<double[]> witnessHeap;

    @SuppressWarnings("unchecked")
    public SpeedyCHBuilder(SpeedyGraph graph, TravelDisutility td) {
        this.graph     = graph;
        this.td        = td;
        this.nodeCount = graph.nodeCount;

        this.nodeLevel                = new int[nodeCount];
        this.contracted               = new boolean[nodeCount];
        this.contractedNeighborCount  = new int[nodeCount];

        int initialCapacity = Math.max(graph.linkCount * 2, 16);
        this.buildEdgeData    = new int[initialCapacity * BE_SIZE];
        this.buildEdgeWeights = new double[initialCapacity];

        this.outEdgeList = new List[nodeCount];
        this.inEdgeList  = new List[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            outEdgeList[i] = new ArrayList<>();
            inEdgeList[i]  = new ArrayList<>();
        }

        this.witnessIterIds = new int[nodeCount];
        this.witnessCost    = new double[nodeCount];
        Arrays.fill(witnessCost, Double.POSITIVE_INFINITY);
        this.witnessHeap = new PriorityQueue<>(
                (a, b) -> Double.compare(a[1], b[1]));
    }

    /** Runs the full CH build pipeline and returns the ready-to-customize graph. */
    public SpeedyCHGraph build() {
        initEdges();
        contractNodes();
        return buildCHGraph();
    }

    // -------------------------------------------------------------------------
    // Phase 1 – import original edges
    // -------------------------------------------------------------------------

    private void initEdges() {
        SpeedyGraph.LinkIterator outLI = graph.getOutLinkIterator();
        for (int node = 0; node < nodeCount; node++) {
            outLI.reset(node);
            while (outLI.next()) {
                int linkIdx = outLI.getLinkIndex();
                int toNode  = outLI.getToNodeIndex();
                double w    = td.getLinkMinimumTravelDisutility(graph.getLink(linkIdx));
                addBuildEdge(node, toNode, linkIdx, -1, -1, -1, w);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 2 – contraction
    // -------------------------------------------------------------------------

    private void contractNodes() {
        // Min-priority queue ordered by contraction priority.
        // Entry: double[]{nodeId, priority}
        PriorityQueue<double[]> pq = new PriorityQueue<>(
                (a, b) -> Double.compare(a[1], b[1]));

        for (int node = 0; node < nodeCount; node++) {
            pq.offer(new double[]{node, computePriority(node)});
        }

        int levelCounter = 0;
        while (!pq.isEmpty()) {
            double[] entry = pq.poll();
            int node = (int) entry[0];
            if (contracted[node]) continue;

            // Lazy update: recompute priority and re-queue if it increased.
            int newPriority = computePriority(node);
            if (newPriority > (int) entry[1]) {
                pq.offer(new double[]{node, newPriority});
                continue;
            }

            // Contract node.
            contractNode(node);
            contracted[node]  = true;
            nodeLevel[node]   = levelCounter++;

            // Update contracted-neighbor counts for remaining neighbors.
            for (int edgeIdx : outEdgeList[node]) {
                int toNode = buildEdgeData[edgeIdx * BE_SIZE + BE_TO];
                if (!contracted[toNode]) contractedNeighborCount[toNode]++;
            }
            for (int edgeIdx : inEdgeList[node]) {
                int fromNode = buildEdgeData[edgeIdx * BE_SIZE + BE_FROM];
                if (!contracted[fromNode]) contractedNeighborCount[fromNode]++;
            }
        }
    }

    private int computePriority(int node) {
        int shortcuts = countShortcutsForContraction(node);
        int removed   = activeInDegree(node) + activeOutDegree(node);
        return shortcuts - removed + contractedNeighborCount[node];
    }

    private int activeOutDegree(int node) {
        int count = 0;
        for (int edgeIdx : outEdgeList[node]) {
            if (!contracted[buildEdgeData[edgeIdx * BE_SIZE + BE_TO]]) count++;
        }
        return count;
    }

    private int activeInDegree(int node) {
        int count = 0;
        for (int edgeIdx : inEdgeList[node]) {
            if (!contracted[buildEdgeData[edgeIdx * BE_SIZE + BE_FROM]]) count++;
        }
        return count;
    }

    private int countShortcutsForContraction(int node) {
        int shortcuts = 0;
        for (int inIdx : inEdgeList[node]) {
            int u = buildEdgeData[inIdx * BE_SIZE + BE_FROM];
            if (contracted[u] || u == node) continue;
            double wUV = buildEdgeWeights[inIdx];
            for (int outIdx : outEdgeList[node]) {
                int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
                if (contracted[w] || w == node || w == u) continue;
                if (!hasWitness(u, w, node, wUV + buildEdgeWeights[outIdx])) {
                    shortcuts++;
                }
            }
        }
        return shortcuts;
    }

    private void contractNode(int node) {
        for (int inIdx : inEdgeList[node]) {
            int u = buildEdgeData[inIdx * BE_SIZE + BE_FROM];
            if (contracted[u] || u == node) continue;
            double wUV = buildEdgeWeights[inIdx];
            for (int outIdx : outEdgeList[node]) {
                int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
                if (contracted[w] || w == node || w == u) continue;
                double maxCost = wUV + buildEdgeWeights[outIdx];
                if (!hasWitness(u, w, node, maxCost)) {
                    // Only add shortcut if no existing edge u→w is at least as cheap.
                    boolean superseded = false;
                    for (int ex : outEdgeList[u]) {
                        if (buildEdgeData[ex * BE_SIZE + BE_TO] == w
                                && buildEdgeWeights[ex] <= maxCost) {
                            superseded = true;
                            break;
                        }
                    }
                    if (!superseded) {
                        addBuildEdge(u, w, -1, node, inIdx, outIdx, maxCost);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Witness search
    // -------------------------------------------------------------------------

    private boolean hasWitness(int source, int target, int avoidNode, double maxCost) {
        return witnessSearch(source, target, avoidNode, maxCost) <= maxCost;
    }

    private double witnessSearch(int source, int target, int avoidNode, double maxCost) {
        witnessIteration++;
        witnessHeap.clear();

        witnessIterIds[source] = witnessIteration;
        witnessCost[source]    = 0.0;
        witnessHeap.offer(new double[]{source, 0.0, 0.0}); // {node, cost, hops}

        while (!witnessHeap.isEmpty()) {
            double[] entry = witnessHeap.poll();
            int    node = (int) entry[0];
            double cost = entry[1];
            int    hops = (int) entry[2];

            // Stale entry: a cheaper path was found later.
            if (witnessIterIds[node] == witnessIteration && cost > witnessCost[node]) {
                continue;
            }
            if (node == target)    return cost;
            if (cost >= maxCost)   continue; // cost bound – cannot improve
            if (hops >= HOP_LIMIT) continue; // hop limit

            for (int edgeIdx : outEdgeList[node]) {
                int toNode = buildEdgeData[edgeIdx * BE_SIZE + BE_TO];
                if (toNode == avoidNode) continue;
                // Only traverse to uncontracted nodes (plus the explicit target).
                if (contracted[toNode] && toNode != target) continue;

                double newCost = cost + buildEdgeWeights[edgeIdx];
                if (newCost > maxCost) continue;

                if (witnessIterIds[toNode] != witnessIteration
                        || newCost < witnessCost[toNode]) {
                    witnessCost[toNode]    = newCost;
                    witnessIterIds[toNode] = witnessIteration;
                    witnessHeap.offer(new double[]{toNode, newCost, hops + 1.0});
                }
            }
        }

        return (witnessIterIds[target] == witnessIteration)
                ? witnessCost[target]
                : Double.POSITIVE_INFINITY;
    }

    // -------------------------------------------------------------------------
    // Phase 3 – build SpeedyCHGraph
    // -------------------------------------------------------------------------

    private SpeedyCHGraph buildCHGraph() {
        int[] nodeData = new int[nodeCount * SpeedyCHGraph.NODE_SIZE];
        Arrays.fill(nodeData, -1);
        int[] edgeData = new int[buildEdgeCount * SpeedyCHGraph.EDGE_SIZE];
        Arrays.fill(edgeData, -1);

        // Fill edge data (next-pointers are set later via linked-list prepend).
        for (int bi = 0; bi < buildEdgeCount; bi++) {
            int bBase = bi * BE_SIZE;
            int cBase = bi * SpeedyCHGraph.EDGE_SIZE;
            edgeData[cBase + 2] = buildEdgeData[bBase + BE_FROM];
            edgeData[cBase + 3] = buildEdgeData[bBase + BE_TO];
            edgeData[cBase + 4] = buildEdgeData[bBase + BE_ORIG];
            edgeData[cBase + 5] = buildEdgeData[bBase + BE_MID];
            // lowerEdge indices are identical to build indices (identity mapping).
            edgeData[cBase + 6] = buildEdgeData[bBase + BE_LOW1];
            edgeData[cBase + 7] = buildEdgeData[bBase + BE_LOW2];
        }

        // Build linked lists by prepending (reverse iteration → head of list is first edge).
        for (int bi = buildEdgeCount - 1; bi >= 0; bi--) {
            int cBase    = bi * SpeedyCHGraph.EDGE_SIZE;
            int fromNode = edgeData[cBase + 2];
            int toNode   = edgeData[cBase + 3];
            int lvFrom   = nodeLevel[fromNode];
            int lvTo     = nodeLevel[toNode];

            if (lvFrom < lvTo) {
                // Upward edge → out-up adjacency of fromNode.
                edgeData[cBase] = nodeData[fromNode * SpeedyCHGraph.NODE_SIZE];
                nodeData[fromNode * SpeedyCHGraph.NODE_SIZE] = bi;
            } else if (lvFrom > lvTo) {
                // Downward edge → down-in adjacency of toNode.
                edgeData[cBase + 1] = nodeData[toNode * SpeedyCHGraph.NODE_SIZE + 1];
                nodeData[toNode * SpeedyCHGraph.NODE_SIZE + 1] = bi;
            }
            // lvFrom == lvTo: self-loop or level-tie – excluded from both adjacencies.
        }

        return new SpeedyCHGraph(graph, nodeCount, buildEdgeCount,
                nodeData, edgeData, nodeLevel);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private int addBuildEdge(int from, int to, int origLink,
                             int middle, int lower1, int lower2, double weight) {
        if (buildEdgeCount * BE_SIZE >= buildEdgeData.length) {
            int newLen = buildEdgeData.length * 2;
            buildEdgeData    = Arrays.copyOf(buildEdgeData, newLen);
            buildEdgeWeights = Arrays.copyOf(buildEdgeWeights, newLen / BE_SIZE);
        }
        int idx  = buildEdgeCount++;
        int base = idx * BE_SIZE;
        buildEdgeData[base + BE_FROM] = from;
        buildEdgeData[base + BE_TO]   = to;
        buildEdgeData[base + BE_ORIG] = origLink;
        buildEdgeData[base + BE_MID]  = middle;
        buildEdgeData[base + BE_LOW1] = lower1;
        buildEdgeData[base + BE_LOW2] = lower2;
        buildEdgeWeights[idx] = weight;
        outEdgeList[from].add(idx);
        inEdgeList[to].add(idx);
        return idx;
    }
}
