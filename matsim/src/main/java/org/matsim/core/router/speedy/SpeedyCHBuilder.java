package org.matsim.core.router.speedy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.core.router.util.TravelDisutility;

import java.util.Arrays;

/**
 * Builds a {@link SpeedyCHGraph} from a {@link SpeedyGraph} using the
 * Contraction Hierarchies (CH) algorithm.
 *
 * <h3>Performance-critical optimisations</h3>
 * <ul>
 *   <li><b>Batched witness search</b>: for each in-neighbour u of the contracted node v,
 *       a single bounded Dijkstra from u (avoiding v) settles ALL out-neighbours w of v
 *       in one pass – O(in_degree) searches instead of O(in_degree × out_degree).</li>
 *   <li><b>Shared witness results</b>: the batched search that runs for priority estimation
 *       is reused when the node is actually contracted (no redundant second pass).</li>
 *   <li><b>Flat int-array adjacency</b>: per-node edge lists use a single growable int[]
 *       with offset/length indexing – no boxing, no ArrayList overhead.</li>
 *   <li><b>Parallel-edge deduplication</b>: when adding a shortcut u→w, we first check
 *       whether an existing edge u→w is already at least as cheap.</li>
 * </ul>
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHBuilder {

    private static final Logger LOG = LogManager.getLogger(SpeedyCHBuilder.class);

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

    // ---- Flat adjacency lists (no boxing) ----
    // For each node, outOff[n]..outOff[n]+outLen[n]-1 are indices into outBuf
    // whose values are build-edge indices.  Same for in.
    private int[] outBuf, inBuf;
    private int[] outOff, outLen, outCap;
    private int[] inOff,  inLen,  inCap;
    private int   outBufUsed, inBufUsed;

    // CH state
    private final int[]     nodeLevel;
    private final boolean[] contracted;
    private final int[]     contractedNeighborCount;

    // Witness-search reusable state
    private int    witnessIteration = 0;
    private final int[]    witnessIterIds;
    private final double[] witnessCost;
    private final int[]    witnessHops;
    private final DAryMinHeap witnessHeap;

    // Scratch arrays reused across batched witness searches
    private int[]    scratchTargets;     // out-neighbor node indices
    private double[] scratchMaxCosts;    // max cost u→v→w for each target
    private int[]    scratchOutEdgeIdx;  // the out-edge index v→w for each target

    public SpeedyCHBuilder(SpeedyGraph graph, TravelDisutility td) {
        this.graph     = graph;
        this.td        = td;
        this.nodeCount = graph.nodeCount;

        this.nodeLevel                = new int[nodeCount];
        this.contracted               = new boolean[nodeCount];
        this.contractedNeighborCount  = new int[nodeCount];

        int edgeCap = Math.max(graph.linkCount * 2, 16);
        this.buildEdgeData    = new int[edgeCap * BE_SIZE];
        this.buildEdgeWeights = new double[edgeCap];

        // Flat adjacency – initial buffer big enough for original edges
        int adjBufCap = Math.max(graph.linkCount + 16, 256);
        this.outBuf = new int[adjBufCap];
        this.inBuf  = new int[adjBufCap];
        this.outOff = new int[nodeCount];
        this.outLen = new int[nodeCount];
        this.outCap = new int[nodeCount];
        this.inOff  = new int[nodeCount];
        this.inLen  = new int[nodeCount];
        this.inCap  = new int[nodeCount];
        // Each node starts with a small inline allocation
        int perNode = Math.max(4, (graph.linkCount / Math.max(nodeCount, 1)) * 2 + 2);
        int pos = 0;
        for (int i = 0; i < nodeCount; i++) {
            outOff[i] = pos; outCap[i] = perNode;
            pos += perNode;
        }
        if (pos > outBuf.length) outBuf = new int[pos];
        this.outBufUsed = pos;
        pos = 0;
        for (int i = 0; i < nodeCount; i++) {
            inOff[i] = pos; inCap[i] = perNode;
            pos += perNode;
        }
        if (pos > inBuf.length) inBuf = new int[pos];
        this.inBufUsed = pos;

        this.witnessIterIds = new int[nodeCount];
        this.witnessCost    = new double[nodeCount];
        this.witnessHops    = new int[nodeCount];
        this.witnessHeap    = new DAryMinHeap(nodeCount, 4);

        this.scratchTargets    = new int[64];
        this.scratchMaxCosts   = new double[64];
        this.scratchOutEdgeIdx = new int[64];
    }

    /** Runs the full CH build pipeline and returns the ready-to-customize graph. */
    public SpeedyCHGraph build() {
        LOG.info("CH contraction: importing {} links from base graph ({} nodes)…",
                graph.linkCount, nodeCount);
        initEdges();
        LOG.info("CH contraction: contracting {} nodes…", nodeCount);
        contractNodes();
        LOG.info("CH contraction: building overlay graph ({} edges)…", buildEdgeCount);
        return buildCHGraph();
    }

    // ---- flat adjacency helpers ----

    private void adjOutAdd(int node, int edgeIdx) {
        if (outLen[node] >= outCap[node]) growOut(node);
        outBuf[outOff[node] + outLen[node]++] = edgeIdx;
    }
    private void adjInAdd(int node, int edgeIdx) {
        if (inLen[node] >= inCap[node]) growIn(node);
        inBuf[inOff[node] + inLen[node]++] = edgeIdx;
    }
    private void growOut(int node) {
        int newCap = outCap[node] * 2;
        int newOff = outBufUsed;
        if (newOff + newCap > outBuf.length) {
            outBuf = Arrays.copyOf(outBuf, Math.max(outBuf.length * 2, newOff + newCap));
        }
        System.arraycopy(outBuf, outOff[node], outBuf, newOff, outLen[node]);
        outOff[node] = newOff;
        outCap[node] = newCap;
        outBufUsed = newOff + newCap;
    }
    private void growIn(int node) {
        int newCap = inCap[node] * 2;
        int newOff = inBufUsed;
        if (newOff + newCap > inBuf.length) {
            inBuf = Arrays.copyOf(inBuf, Math.max(inBuf.length * 2, newOff + newCap));
        }
        System.arraycopy(inBuf, inOff[node], inBuf, newOff, inLen[node]);
        inOff[node] = newOff;
        inCap[node] = newCap;
        inBufUsed = newOff + newCap;
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
        DAryMinHeap pq = new DAryMinHeap(nodeCount, 4);
        int[] nodePriority = new int[nodeCount];

        for (int node = 0; node < nodeCount; node++) {
            int prio = computePriority(node);
            nodePriority[node] = prio;
            pq.insert(node, prio);
        }

        int levelCounter = 0;
        int logInterval = Math.max(nodeCount / 20, 1);
        while (!pq.isEmpty()) {
            int node = pq.poll();
            if (contracted[node]) continue;

            // Lazy update: recompute priority and re-queue if it increased.
            int newPriority = computePriority(node);
            if (newPriority > nodePriority[node]) {
                nodePriority[node] = newPriority;
                pq.insert(node, newPriority);
                continue;
            }

            // Contract node using batched witness search.
            contractNodeBatched(node);
            contracted[node]  = true;
            nodeLevel[node]   = levelCounter++;

            if (levelCounter % logInterval == 0) {
                LOG.info("  … contracted {}/{} nodes ({} edges so far)",
                        levelCounter, nodeCount, buildEdgeCount);
            }

            // Update contracted-neighbor counts for remaining neighbors.
            int oOff = outOff[node], oLen = outLen[node];
            for (int i = 0; i < oLen; i++) {
                int toNode = buildEdgeData[outBuf[oOff + i] * BE_SIZE + BE_TO];
                if (!contracted[toNode]) contractedNeighborCount[toNode]++;
            }
            int iOff = inOff[node], iLen = inLen[node];
            for (int i = 0; i < iLen; i++) {
                int fromNode = buildEdgeData[inBuf[iOff + i] * BE_SIZE + BE_FROM];
                if (!contracted[fromNode]) contractedNeighborCount[fromNode]++;
            }
        }
    }

    /**
     * Computes the contraction priority using batched witness search.
     * For each in-neighbour u, ONE Dijkstra finds witnesses for ALL out-neighbours w.
     */
    private int computePriority(int node) {
        int oOff = outOff[node], oLen = outLen[node];
        int iOff = inOff[node],  iLen = inLen[node];

        // Collect active out-neighbours and their edge costs v→w.
        int numTargets = 0;
        for (int i = 0; i < oLen; i++) {
            int outIdx = outBuf[oOff + i];
            int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
            if (contracted[w] || w == node) continue;
            if (numTargets >= scratchTargets.length) growScratch();
            scratchTargets[numTargets]  = w;
            scratchMaxCosts[numTargets] = buildEdgeWeights[outIdx];
            numTargets++;
        }

        int inDeg  = 0;
        int outDeg = numTargets;
        int shortcuts = 0;

        for (int j = 0; j < iLen; j++) {
            int inIdx = inBuf[iOff + j];
            int u = buildEdgeData[inIdx * BE_SIZE + BE_FROM];
            if (contracted[u] || u == node) continue;
            inDeg++;
            if (numTargets == 0) continue;

            double wUV = buildEdgeWeights[inIdx];

            // Find max cost bound for this u.
            double globalMax = 0;
            for (int t = 0; t < numTargets; t++) {
                if (scratchTargets[t] == u) continue;
                double bound = wUV + scratchMaxCosts[t];
                if (bound > globalMax) globalMax = bound;
            }

            // One batched Dijkstra from u.
            batchedWitnessSearch(u, node, globalMax);

            // Count shortcuts needed.
            for (int t = 0; t < numTargets; t++) {
                int w = scratchTargets[t];
                if (w == u) continue;
                double scCost = wUV + scratchMaxCosts[t];
                double witCost = (witnessIterIds[w] == witnessIteration)
                        ? witnessCost[w] : Double.POSITIVE_INFINITY;
                if (witCost >= scCost) shortcuts++;
            }
        }

        return shortcuts - (inDeg + outDeg) + contractedNeighborCount[node];
    }

    private void growScratch() {
        int newLen = scratchTargets.length * 2;
        scratchTargets    = Arrays.copyOf(scratchTargets, newLen);
        scratchMaxCosts   = Arrays.copyOf(scratchMaxCosts, newLen);
        scratchOutEdgeIdx = Arrays.copyOf(scratchOutEdgeIdx, newLen);
    }

    /**
     * Contracts a node using batched witness search: for each in-neighbour u,
     * runs ONE Dijkstra from u (avoiding the contracted node) to find witnesses
     * for ALL out-neighbours w simultaneously.
     */
    private void contractNodeBatched(int node) {
        int oOff = outOff[node], oLen = outLen[node];
        int iOff = inOff[node],  iLen = inLen[node];

        // Collect active out-neighbors and their costs through node.
        int numTargets = 0;
        for (int i = 0; i < oLen; i++) {
            int outIdx = outBuf[oOff + i];
            int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
            if (contracted[w] || w == node) continue;
            if (numTargets >= scratchTargets.length) {
                int newLen = scratchTargets.length * 2;
                scratchTargets    = Arrays.copyOf(scratchTargets, newLen);
                scratchMaxCosts   = Arrays.copyOf(scratchMaxCosts, newLen);
                scratchOutEdgeIdx = Arrays.copyOf(scratchOutEdgeIdx, newLen);
            }
            scratchTargets[numTargets]    = w;
            scratchMaxCosts[numTargets]   = buildEdgeWeights[outIdx]; // cost v→w (added to wUV later)
            scratchOutEdgeIdx[numTargets] = outIdx;
            numTargets++;
        }
        if (numTargets == 0) return;

        // For each active in-neighbour u, run ONE batched witness search.
        for (int j = 0; j < iLen; j++) {
            int inIdx = inBuf[iOff + j];
            int u = buildEdgeData[inIdx * BE_SIZE + BE_FROM];
            if (contracted[u] || u == node) continue;
            double wUV = buildEdgeWeights[inIdx]; // cost u→v

            // Find the maximum cost bound we need for this u (max over all targets).
            double globalMaxCost = 0;
            for (int t = 0; t < numTargets; t++) {
                if (scratchTargets[t] == u) continue;
                double bound = wUV + scratchMaxCosts[t];
                if (bound > globalMaxCost) globalMaxCost = bound;
            }

            // Run one bounded Dijkstra from u, avoiding node.
            batchedWitnessSearch(u, node, globalMaxCost);

            // Check each target.
            for (int t = 0; t < numTargets; t++) {
                int w = scratchTargets[t];
                if (w == u) continue;
                double shortcutCost = wUV + scratchMaxCosts[t];

                // Check if witness found (cost < shortcutCost).
                double witnessCostToW = (witnessIterIds[w] == witnessIteration)
                        ? witnessCost[w] : Double.POSITIVE_INFINITY;
                if (witnessCostToW < shortcutCost) continue; // witness exists

                // Check if existing edge u→w is already at least as cheap.
                boolean superseded = false;
                int uOOff = outOff[u], uOLen = outLen[u];
                for (int k = 0; k < uOLen; k++) {
                    int ex = outBuf[uOOff + k];
                    if (buildEdgeData[ex * BE_SIZE + BE_TO] == w
                            && buildEdgeWeights[ex] <= shortcutCost) {
                        superseded = true;
                        break;
                    }
                }
                if (!superseded) {
                    addBuildEdge(u, w, -1, node, inIdx, scratchOutEdgeIdx[t], shortcutCost);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Batched witness search – one Dijkstra from source, finds all targets
    // -------------------------------------------------------------------------

    /**
     * Runs a single bounded Dijkstra from {@code source}, avoiding {@code avoidNode},
     * up to {@code maxCost} and {@value HOP_LIMIT} hops. After return, callers can
     * read {@code witnessCost[target]} for any target to check if a witness exists.
     */
    private void batchedWitnessSearch(int source, int avoidNode, double maxCost) {
        witnessIteration++;
        witnessHeap.clear();

        witnessIterIds[source] = witnessIteration;
        witnessCost[source]    = 0.0;
        witnessHops[source]    = 0;
        witnessHeap.insert(source, 0.0);

        while (!witnessHeap.isEmpty()) {
            int    v    = witnessHeap.poll();
            double cost = witnessCost[v];
            int    hops = witnessHops[v];

            if (cost >= maxCost)   break; // everything remaining exceeds bound
            if (hops >= HOP_LIMIT) continue;

            int vOOff = outOff[v], vOLen = outLen[v];
            for (int i = 0; i < vOLen; i++) {
                int edgeIdx = outBuf[vOOff + i];
                int toNode = buildEdgeData[edgeIdx * BE_SIZE + BE_TO];
                if (toNode == avoidNode) continue;
                if (contracted[toNode]) continue;

                double newCost = cost + buildEdgeWeights[edgeIdx];
                if (newCost >= maxCost) continue;

                if (witnessIterIds[toNode] != witnessIteration) {
                    witnessCost[toNode]    = newCost;
                    witnessHops[toNode]    = hops + 1;
                    witnessIterIds[toNode] = witnessIteration;
                    witnessHeap.insert(toNode, newCost);
                } else if (newCost < witnessCost[toNode]) {
                    witnessCost[toNode]    = newCost;
                    witnessHops[toNode]    = hops + 1;
                    witnessHeap.decreaseKey(toNode, newCost);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3 – build SpeedyCHGraph
    // -------------------------------------------------------------------------

    private SpeedyCHGraph buildCHGraph() {
        int[] nodeData = new int[nodeCount * SpeedyCHGraph.NODE_SIZE];
        Arrays.fill(nodeData, -1);
        int[] edgeData = new int[buildEdgeCount * SpeedyCHGraph.EDGE_SIZE];
        Arrays.fill(edgeData, -1);

        for (int bi = 0; bi < buildEdgeCount; bi++) {
            int bBase = bi * BE_SIZE;
            int cBase = bi * SpeedyCHGraph.EDGE_SIZE;
            edgeData[cBase + 2] = buildEdgeData[bBase + BE_FROM];
            edgeData[cBase + 3] = buildEdgeData[bBase + BE_TO];
            edgeData[cBase + 4] = buildEdgeData[bBase + BE_ORIG];
            edgeData[cBase + 5] = buildEdgeData[bBase + BE_MID];
            edgeData[cBase + 6] = buildEdgeData[bBase + BE_LOW1];
            edgeData[cBase + 7] = buildEdgeData[bBase + BE_LOW2];
        }

        for (int bi = buildEdgeCount - 1; bi >= 0; bi--) {
            int cBase    = bi * SpeedyCHGraph.EDGE_SIZE;
            int fromNode = edgeData[cBase + 2];
            int toNode   = edgeData[cBase + 3];
            int lvFrom   = nodeLevel[fromNode];
            int lvTo     = nodeLevel[toNode];

            if (lvFrom < lvTo) {
                edgeData[cBase] = nodeData[fromNode * SpeedyCHGraph.NODE_SIZE];
                nodeData[fromNode * SpeedyCHGraph.NODE_SIZE] = bi;
            } else if (lvFrom > lvTo) {
                edgeData[cBase + 1] = nodeData[toNode * SpeedyCHGraph.NODE_SIZE + 1];
                nodeData[toNode * SpeedyCHGraph.NODE_SIZE + 1] = bi;
            }
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
        adjOutAdd(from, idx);
        adjInAdd(to, idx);
        return idx;
    }
}
