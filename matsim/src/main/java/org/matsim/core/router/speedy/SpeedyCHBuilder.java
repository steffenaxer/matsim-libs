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
    private static final int HOP_LIMIT = 10;

    /** Maximum number of nodes settled in a single witness search.
     *  Prevents unbounded exploration on large networks. */
    private static final int SETTLED_LIMIT = 1000;

    /** Cheaper hop/settled limits for priority estimation witness search. */
    private static final int PRIO_HOP_LIMIT = 3;
    private static final int PRIO_SETTLED_LIMIT = 200;

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

    // Dedup lookup: generation-stamped best-cost for existing out-edges from source u.
    // dedupGeneration is bumped per in-neighbour u; dedupGen[w] == dedupGeneration
    // means dedupBest[w] holds the cost of the cheapest existing edge u→w.
    private int        dedupGeneration = 0;
    private final int[]    dedupGen;
    private final double[] dedupBest;

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

        this.dedupGen  = new int[nodeCount];
        this.dedupBest = new double[nodeCount];

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

    /**
     * Builds a CH using a pre-computed contraction order (e.g. from nested dissection).
     *
     * <p>Nodes are contracted in the order specified: {@code order[nodeIndex]} gives the
     * contraction level for that node (0 = first contracted, nodeCount-1 = last contracted).
     * No priority-queue witness search is used for ordering; only the contraction-time
     * witness search runs (to determine which shortcuts are needed).
     *
     * <p>This is much faster than {@link #build()} because it eliminates the expensive
     * priority estimation step, at the cost of potentially more shortcuts.
     *
     * @param order contraction order; {@code order[node]} is the level/rank for that node.
     * @return the ready-to-customize CH graph.
     */
    public SpeedyCHGraph buildWithOrder(int[] order) {
        LOG.info("CH contraction (fixed order): importing {} links from base graph ({} nodes)…",
                graph.linkCount, nodeCount);
        initEdges();

        LOG.info("CH contraction (fixed order): contracting {} nodes…", nodeCount);
        contractNodesInOrder(order);

        LOG.info("CH contraction (fixed order): building overlay graph ({} edges)…", buildEdgeCount);
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
            int prio = estimatePriority(node);
            nodePriority[node] = prio;
            pq.insert(node, prio);
        }

        int levelCounter = 0;
        int logInterval = Math.max(nodeCount / 20, 1);
        while (!pq.isEmpty()) {
            int node = pq.poll();
            if (contracted[node]) continue;

            // Lazy update: recompute cheap priority estimate and re-queue if it increased.
            int newPriority = estimatePriority(node);
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
     * Contracts nodes in a fixed, pre-computed order. No priority estimation or
     * lazy updates – just contract in sequence. The witness search still runs
     * during contraction to determine necessary shortcuts.
     */
    private void contractNodesInOrder(int[] order) {
        // Build inverse order: sortedNodes[level] = nodeIndex
        int[] sortedNodes = new int[nodeCount];
        for (int node = 0; node < nodeCount; node++) {
            sortedNodes[order[node]] = node;
        }

        int logInterval = Math.max(nodeCount / 20, 1);
        for (int level = 0; level < nodeCount; level++) {
            int node = sortedNodes[level];

            contractNodeBatched(node);
            contracted[node] = true;
            nodeLevel[node]  = level;

            if ((level + 1) % logInterval == 0) {
                LOG.info("  … contracted {}/{} nodes ({} edges so far)",
                        level + 1, nodeCount, buildEdgeCount);
            }
        }
    }

    /**
     * Priority estimation using a quick, limited witness search.
     * Uses reduced hop/settled limits compared to the full contraction witness search.
     * This gives a much tighter shortcut count estimate than the inDeg × outDeg upper bound
     * without the full cost of the contraction-time search.
     */
    private int estimatePriority(int node) {
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

            // Quick bounded Dijkstra from u with reduced limits.
            batchedWitnessSearch(u, node, globalMax, PRIO_HOP_LIMIT, PRIO_SETTLED_LIMIT);

            // Count shortcuts needed.
            for (int t = 0; t < numTargets; t++) {
                int w = scratchTargets[t];
                if (w == u) continue;
                double scCost = wUV + scratchMaxCosts[t];
                double witCost = (witnessIterIds[w] == witnessIteration)
                        ? witnessCost[w] : Double.POSITIVE_INFINITY;
                if (witCost <= scCost) continue; // witness exists
                shortcuts++;
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
            batchedWitnessSearch(u, node, globalMaxCost, HOP_LIMIT, SETTLED_LIMIT);

            // Build O(1) dedup lookup: best existing out-edge cost from u to each neighbor.
            dedupGeneration++;
            int uOOff = outOff[u], uOLen = outLen[u];
            for (int k = 0; k < uOLen; k++) {
                int ex = outBuf[uOOff + k];
                int target = buildEdgeData[ex * BE_SIZE + BE_TO];
                double eCost = buildEdgeWeights[ex];
                if (dedupGen[target] != dedupGeneration || eCost < dedupBest[target]) {
                    dedupGen[target]  = dedupGeneration;
                    dedupBest[target] = eCost;
                }
            }

            // Check each target.
            for (int t = 0; t < numTargets; t++) {
                int w = scratchTargets[t];
                if (w == u) continue;
                double shortcutCost = wUV + scratchMaxCosts[t];

                // Check if witness found (cost <= shortcutCost means witness exists).
                double witnessCostToW = (witnessIterIds[w] == witnessIteration)
                        ? witnessCost[w] : Double.POSITIVE_INFINITY;
                if (witnessCostToW <= shortcutCost) continue; // witness exists

                // O(1) check: existing edge u→w already at least as cheap?
                if (dedupGen[w] == dedupGeneration && dedupBest[w] <= shortcutCost) continue;

                addBuildEdge(u, w, -1, node, inIdx, scratchOutEdgeIdx[t], shortcutCost);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Batched witness search – one Dijkstra from source, finds all targets
    // -------------------------------------------------------------------------

    /**
     * Runs a single bounded Dijkstra from {@code source}, avoiding {@code avoidNode},
     * up to {@code maxCost} and {@code hopLimit} hops. After return, callers can
     * read {@code witnessCost[target]} for any target to check if a witness exists.
     */
    private void batchedWitnessSearch(int source, int avoidNode, double maxCost,
                                      int hopLimit, int settledLimit) {
        witnessIteration++;
        witnessHeap.clear();

        witnessIterIds[source] = witnessIteration;
        witnessCost[source]    = 0.0;
        witnessHops[source]    = 0;
        witnessHeap.insert(source, 0.0);

        int settled = 0;
        while (!witnessHeap.isEmpty()) {
            int    v    = witnessHeap.poll();
            double cost = witnessCost[v];
            int    hops = witnessHops[v];

            if (cost > maxCost)   break; // everything remaining exceeds bound
            if (hops >= hopLimit) continue;
            if (++settled > settledLimit) break; // prevent unbounded exploration

            int vOOff = outOff[v], vOLen = outLen[v];
            for (int i = 0; i < vOLen; i++) {
                int edgeIdx = outBuf[vOOff + i];
                int toNode = buildEdgeData[edgeIdx * BE_SIZE + BE_TO];
                if (toNode == avoidNode) continue;
                if (contracted[toNode]) continue;

                double newCost = cost + buildEdgeWeights[edgeIdx];
                if (newCost > maxCost) continue;

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
    // Phase 3 – build SpeedyCHGraph (CSR layout)
    // -------------------------------------------------------------------------

    private SpeedyCHGraph buildCHGraph() {
        // 1. Count up/dn edges per node
        int[] upCount = new int[nodeCount];
        int[] dnCount = new int[nodeCount];
        for (int bi = 0; bi < buildEdgeCount; bi++) {
            int bBase    = bi * BE_SIZE;
            int fromNode = buildEdgeData[bBase + BE_FROM];
            int toNode   = buildEdgeData[bBase + BE_TO];
            int lvFrom   = nodeLevel[fromNode];
            int lvTo     = nodeLevel[toNode];
            if (lvFrom < lvTo) {
                upCount[fromNode]++;
            } else if (lvFrom > lvTo) {
                dnCount[toNode]++;
            }
        }

        // 2. Compute CSR offsets
        int[] upOff = new int[nodeCount];
        int[] dnOff = new int[nodeCount];
        int totalUp = 0, totalDn = 0;
        for (int n = 0; n < nodeCount; n++) {
            upOff[n] = totalUp; totalUp += upCount[n];
            dnOff[n] = totalDn; totalDn += dnCount[n];
        }

        // 3. Allocate CSR edge arrays and colocated weight arrays
        // CSR stores only toNode + globalIdx (E_STRIDE = 2) for cache-compact hot path.
        int S = SpeedyCHGraph.E_STRIDE;
        int[] upEdges      = new int[totalUp * S];
        double[] upWeights = new double[totalUp];
        int[] dnEdges      = new int[totalDn * S];
        double[] dnWeights = new double[totalDn];

        // Total edges = up + dn (every build edge goes into exactly one list)
        int totalEdgeCount = totalUp + totalDn;

        // Global edge arrays indexed by new contiguous gIdx:
        //   up-edges: gIdx = slot [0, totalUp)
        //   dn-edges: gIdx = totalUp + dnSlot [totalUp, totalEdgeCount)
        int[] edgeOrigLink = new int[totalEdgeCount];
        int[] edgeLower1   = new int[totalEdgeCount];
        int[] edgeLower2   = new int[totalEdgeCount];

        // Build old-to-new index mapping for gIdx remapping
        int[] oldToNew = new int[buildEdgeCount];
        Arrays.fill(oldToNew, -1);

        // 4. Fill CSR arrays (use upCount/dnCount as cursors, reset to 0 first)
        int[] upCursor = new int[nodeCount];
        int[] dnCursor = new int[nodeCount];

        for (int bi = 0; bi < buildEdgeCount; bi++) {
            int bBase    = bi * BE_SIZE;
            int fromNode = buildEdgeData[bBase + BE_FROM];
            int toNode   = buildEdgeData[bBase + BE_TO];
            int origLink = buildEdgeData[bBase + BE_ORIG];
            int lvFrom   = nodeLevel[fromNode];
            int lvTo     = nodeLevel[toNode];

            if (lvFrom < lvTo) {
                // Upward edge: gIdx = slot
                int slot = upOff[fromNode] + upCursor[fromNode]++;
                int eBase = slot * S;
                upEdges[eBase + SpeedyCHGraph.E_NODE] = toNode;
                upEdges[eBase + SpeedyCHGraph.E_GIDX] = slot; // contiguous gIdx
                upWeights[slot] = buildEdgeWeights[bi];
                edgeOrigLink[slot] = origLink;
                // lower edges will be remapped after this loop
                edgeLower1[slot] = buildEdgeData[bBase + BE_LOW1];
                edgeLower2[slot] = buildEdgeData[bBase + BE_LOW2];
                oldToNew[bi] = slot;
            } else if (lvFrom > lvTo) {
                // Downward edge: gIdx = totalUp + dnSlot
                int dnSlot = dnOff[toNode] + dnCursor[toNode]++;
                int gIdx = totalUp + dnSlot;
                int eBase = dnSlot * S;
                dnEdges[eBase + SpeedyCHGraph.E_NODE] = fromNode;
                dnEdges[eBase + SpeedyCHGraph.E_GIDX] = gIdx; // contiguous gIdx
                dnWeights[dnSlot] = buildEdgeWeights[bi];
                edgeOrigLink[gIdx] = origLink;
                edgeLower1[gIdx] = buildEdgeData[bBase + BE_LOW1];
                edgeLower2[gIdx] = buildEdgeData[bBase + BE_LOW2];
                oldToNew[bi] = gIdx;
            }
        }

        // 5. Remap edgeLower1/edgeLower2 from old build-edge indices to new gIdx
        for (int e = 0; e < totalEdgeCount; e++) {
            if (edgeLower1[e] >= 0) {
                edgeLower1[e] = oldToNew[edgeLower1[e]];
            }
            if (edgeLower2[e] >= 0) {
                edgeLower2[e] = oldToNew[edgeLower2[e]];
            }
        }

        // 6. Build topological customization order:
        // Process build edges in original order (which preserves dependency: lower edges
        // are created before their parent shortcuts). Map to new gIdx.
        int[] customizeOrder = new int[totalEdgeCount];
        int orderIdx = 0;
        for (int bi = 0; bi < buildEdgeCount; bi++) {
            if (oldToNew[bi] >= 0) {
                customizeOrder[orderIdx++] = oldToNew[bi];
            }
        }

        return new SpeedyCHGraph(graph, nodeCount,
                totalUp, upOff, upCount, upEdges, upWeights,
                totalDn, dnOff, dnCount, dnEdges, dnWeights,
                totalEdgeCount, edgeOrigLink, edgeLower1, edgeLower2,
                customizeOrder);
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
