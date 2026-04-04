package org.matsim.core.router.speedy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.core.router.util.TravelDisutility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger buildEdgeCounter = new AtomicInteger(0);
    private volatile int[] buildEdgeData;
    private volatile double[] buildEdgeWeights;

    // ---- Per-node adjacency lists (no global buffer) ----
    // outEdges[n] is a growable int[] of build-edge indices for out-edges of n.
    // outLen[n] is the number of valid entries.  Same for in.
    private int[][] outEdges;
    private int[]   outLen;
    private int[][] inEdges;
    private int[]   inLen;
    private Object[] adjLocks;  // per-node locks for adjacency list mutation

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

    /**
     * Per-thread witness search state, used for parallel contraction.
     * Each thread gets its own context to avoid sharing mutable state.
     *
     * <p>Includes a shortcut buffer that collects shortcuts discovered during
     * witness search.  The buffer is flushed to the shared graph in batch,
     * reducing lock acquisitions from O(shortcuts) to O(nodes).
     */
    static class WitnessContext {
        int witnessIteration = 0;
        final int[] witnessIterIds;
        final double[] witnessCost;
        final int[] witnessHops;
        final DAryMinHeap witnessHeap;

        int dedupGeneration = 0;
        final int[] dedupGen;
        final double[] dedupBest;

        int[] scratchTargets = new int[64];
        double[] scratchMaxCosts = new double[64];
        int[] scratchOutEdgeIdx = new int[64];

        // Shortcut collection buffer — flushed per contracted node.
        int scCount = 0;
        int[] scFrom  = new int[64];
        int[] scTo    = new int[64];
        int[] scMid   = new int[64];
        int[] scLow1  = new int[64];
        int[] scLow2  = new int[64];
        double[] scCost = new double[64];

        WitnessContext(int nodeCount) {
            this.witnessIterIds = new int[nodeCount];
            this.witnessCost = new double[nodeCount];
            this.witnessHops = new int[nodeCount];
            this.witnessHeap = new DAryMinHeap(nodeCount, 4);
            this.dedupGen = new int[nodeCount];
            this.dedupBest = new double[nodeCount];
        }

        void growScratch() {
            int newLen = scratchTargets.length * 2;
            scratchTargets    = Arrays.copyOf(scratchTargets, newLen);
            scratchMaxCosts   = Arrays.copyOf(scratchMaxCosts, newLen);
            scratchOutEdgeIdx = Arrays.copyOf(scratchOutEdgeIdx, newLen);
        }

        void addShortcut(int from, int to, int mid, int low1, int low2, double cost) {
            if (scCount >= scFrom.length) growShortcutBuf();
            scFrom[scCount] = from;
            scTo[scCount]   = to;
            scMid[scCount]  = mid;
            scLow1[scCount] = low1;
            scLow2[scCount] = low2;
            scCost[scCount] = cost;
            scCount++;
        }

        private void growShortcutBuf() {
            int newLen = scFrom.length * 2;
            scFrom = Arrays.copyOf(scFrom, newLen);
            scTo   = Arrays.copyOf(scTo, newLen);
            scMid  = Arrays.copyOf(scMid, newLen);
            scLow1 = Arrays.copyOf(scLow1, newLen);
            scLow2 = Arrays.copyOf(scLow2, newLen);
            scCost = Arrays.copyOf(scCost, newLen);
        }
    }

    public SpeedyCHBuilder(SpeedyGraph graph, TravelDisutility td) {
        this.graph     = graph;
        this.td        = td;
        this.nodeCount = graph.nodeCount;

        this.nodeLevel                = new int[nodeCount];
        this.contracted               = new boolean[nodeCount];
        this.contractedNeighborCount  = new int[nodeCount];

        // Pre-size generously to avoid reallocation during contraction.
        // Typical CH produces 3-5× the original edges.
        int edgeCap = Math.max(graph.linkCount * 5, 16);
        this.buildEdgeData    = new int[edgeCap * BE_SIZE];
        this.buildEdgeWeights = new double[edgeCap];

        // Per-node adjacency lists – small initial capacity per node
        int perNode = Math.max(4, (graph.linkCount / Math.max(nodeCount, 1)) * 2 + 2);
        this.outEdges = new int[nodeCount][];
        this.outLen   = new int[nodeCount];
        this.inEdges  = new int[nodeCount][];
        this.inLen    = new int[nodeCount];
        this.adjLocks = new Object[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            outEdges[i] = new int[perNode];
            inEdges[i]  = new int[perNode];
            adjLocks[i] = new Object();
        }

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
        LOG.info("CH contraction: building overlay graph ({} edges)…", buildEdgeCounter.get());
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

        LOG.info("CH contraction (fixed order): building overlay graph ({} edges)…", buildEdgeCounter.get());
        return buildCHGraph();
    }

    /**
     * Builds a CH using a pre-computed contraction order with <b>parallel</b>
     * contraction of independent nested-dissection cells.
     *
     * <p>Within each round, cells are contracted in parallel using a
     * {@link ForkJoinPool}.  Between rounds, a barrier ensures that all
     * cells at the current ND depth are fully contracted before moving
     * to the next (shallower) depth.
     *
     * @param orderResult result from {@link InertialFlowCutter#computeOrderWithBatches()}
     * @return the ready-to-customize CH graph.
     */
    public SpeedyCHGraph buildWithOrderParallel(InertialFlowCutter.NDOrderResult orderResult) {
        LOG.info("CH contraction (parallel): importing {} links from base graph ({} nodes)…",
                graph.linkCount, nodeCount);
        initEdges();

        LOG.info("CH contraction (parallel): contracting {} nodes with {} rounds, {} threads…",
                nodeCount, orderResult.rounds.size(),
                Runtime.getRuntime().availableProcessors());
        contractNodesParallel(orderResult.order, orderResult.rounds);

        LOG.info("CH contraction (parallel): building overlay graph ({} edges)…", buildEdgeCounter.get());
        return buildCHGraph();
    }

    // ---- per-node adjacency helpers ----

    private void adjOutAdd(int node, int edgeIdx) {
        synchronized (adjLocks[node]) {
            int len = outLen[node];
            int[] arr = outEdges[node];
            if (len >= arr.length) {
                outEdges[node] = arr = Arrays.copyOf(arr, arr.length * 2);
            }
            arr[len] = edgeIdx;
            outLen[node] = len + 1;
        }
    }
    private void adjInAdd(int node, int edgeIdx) {
        synchronized (adjLocks[node]) {
            int len = inLen[node];
            int[] arr = inEdges[node];
            if (len >= arr.length) {
                inEdges[node] = arr = Arrays.copyOf(arr, arr.length * 2);
            }
            arr[len] = edgeIdx;
            inLen[node] = len + 1;
        }
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
                        levelCounter, nodeCount, buildEdgeCounter.get());
            }

            // Update contracted-neighbor counts for remaining neighbors.
            int[] oArr = outEdges[node];
            int oLen = outLen[node];
            for (int i = 0; i < oLen; i++) {
                int toNode = buildEdgeData[oArr[i] * BE_SIZE + BE_TO];
                if (!contracted[toNode]) contractedNeighborCount[toNode]++;
            }
            int[] iArr = inEdges[node];
            int iLen = inLen[node];
            for (int i = 0; i < iLen; i++) {
                int fromNode = buildEdgeData[iArr[i] * BE_SIZE + BE_FROM];
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
                        level + 1, nodeCount, buildEdgeCounter.get());
            }
        }
    }

    /**
     * Contracts nodes in parallel using the ND partition structure.
     *
     * <p>Each round contains independent cells that can be contracted in
     * parallel.  Within each cell, nodes are contracted sequentially.
     * A barrier between rounds ensures all cells at the current ND depth
     * are fully contracted before moving to the next (shallower) depth.
     *
     * <h3>Two-level parallelism strategy</h3>
     * <ul>
     *   <li><b>Inter-cell parallelism</b> – rounds with many cells (deep ND
     *       levels) distribute cells across threads.  Each thread reuses a
     *       {@code ThreadLocal} {@link WitnessContext} to avoid GC pressure.</li>
     *   <li><b>Intra-node parallelism</b> – rounds with ≤ nThreads cells (shallow
     *       ND levels, especially the root separator) parallelize the independent
     *       witness searches <em>within</em> each node contraction across
     *       in-neighbours.  This is critical because the shallow rounds dominate
     *       runtime (high-degree separator nodes, dense graph) but have few cells
     *       and therefore no inter-cell parallelism.</li>
     *   <li><b>Batched edge addition</b> – shortcuts are collected in thread-local
     *       buffers and flushed under one lock per contracted node.</li>
     * </ul>
     */
    private void contractNodesParallel(int[] order, List<List<int[]>> rounds) {
        int nThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
        ForkJoinPool pool = (nThreads > 1) ? new ForkJoinPool(nThreads) : null;

        // Reuse one WitnessContext per thread across all rounds & tasks.
        ThreadLocal<WitnessContext> tlCtx = ThreadLocal.withInitial(() -> new WitnessContext(nodeCount));

        int totalContracted = 0;

        for (int r = 0; r < rounds.size(); r++) {
            List<int[]> round = rounds.get(r);
            int roundNodes = roundNodeCount(round);

            // Merge very small cells into chunks for better load balancing.
            List<int[]> chunks = mergeSmallCells(round, 500);

            if (pool == null) {
                // No thread pool: everything sequential
                for (int[] chunk : chunks) {
                    contractCellSequential(chunk, order);
                }
            } else if (chunks.size() > nThreads) {
                // Many independent chunks: use inter-cell parallelism
                List<ForkJoinTask<?>> tasks = new ArrayList<>(chunks.size());
                for (int[] chunk : chunks) {
                    tasks.add(pool.submit(() -> {
                        WitnessContext ctx = tlCtx.get();
                        contractCellWithContext(chunk, order, ctx);
                    }));
                }
                for (ForkJoinTask<?> task : tasks) {
                    task.join();
                }
            } else {
                // Few cells (shallow ND level): use intra-node parallelism.
                // Nodes within a cell are contracted sequentially, but each
                // node's witness searches across in-neighbours run in parallel.
                for (int[] chunk : chunks) {
                    contractCellIntraParallel(chunk, order, pool, tlCtx);
                }
            }

            totalContracted += roundNodes;
            LOG.info("  … contracted {}/{} nodes ({}%), depth {}/{} ({} edges so far)",
                    totalContracted, nodeCount,
                    (int) (100.0 * totalContracted / nodeCount),
                    r + 1, rounds.size(), buildEdgeCounter.get());
        }

        if (pool != null) {
            pool.shutdown();
        }
    }

    private static int roundNodeCount(List<int[]> round) {
        int count = 0;
        for (int[] cell : round) count += cell.length;
        return count;
    }

    /**
     * Merge small cells into larger chunks for better task granularity.
     * Adjacent cells in the list are combined until the chunk reaches
     * {@code minChunkSize} nodes.
     */
    private static List<int[]> mergeSmallCells(List<int[]> cells, int minChunkSize) {
        if (cells.size() <= 1) return cells;

        List<int[]> chunks = new ArrayList<>();
        List<int[]> pending = new ArrayList<>();
        int pendingSize = 0;

        for (int[] cell : cells) {
            pending.add(cell);
            pendingSize += cell.length;
            if (pendingSize >= minChunkSize) {
                chunks.add(flattenCells(pending));
                pending.clear();
                pendingSize = 0;
            }
        }
        if (!pending.isEmpty()) {
            chunks.add(flattenCells(pending));
        }
        return chunks;
    }

    private static int[] flattenCells(List<int[]> cells) {
        if (cells.size() == 1) return cells.get(0);
        int total = 0;
        for (int[] c : cells) total += c.length;
        int[] result = new int[total];
        int pos = 0;
        for (int[] c : cells) {
            System.arraycopy(c, 0, result, pos, c.length);
            pos += c.length;
        }
        return result;
    }

    /** Contract a cell sequentially using the builder's own witness state. */
    private void contractCellSequential(int[] cellNodes, int[] order) {
        for (int node : cellNodes) {
            contractNodeBatched(node);
            contracted[node] = true;
            nodeLevel[node]  = order[node];
        }
    }

    /**
     * Contract a cell using a thread-local {@link WitnessContext}.
     * The graph mutation (addBuildEdge) is synchronized to prevent
     * concurrent corruption of shared adjacency structures.
     */
    private void contractCellWithContext(int[] cellNodes, int[] order, WitnessContext ctx) {
        for (int node : cellNodes) {
            contractNodeBatchedCtx(node, ctx);
            contracted[node] = true;
            nodeLevel[node]  = order[node];
        }
    }

    /**
     * Minimum active in-degree for a node to use intra-node parallelism.
     * Below this threshold, the overhead of task submission exceeds the
     * benefit of parallel witness searches.
     */
    private static final int INTRA_PAR_MIN_IN_DEGREE = 6;

    /**
     * Contract a cell using <b>intra-node witness search parallelism</b>.
     *
     * <p>Nodes are still contracted sequentially (dependency requirement),
     * but for each high-degree node, the independent witness searches from
     * different in-neighbours are dispatched in parallel across the pool.
     *
     * <p>This is critical for the root separator and shallow ND levels where
     * there are few cells but many high-degree nodes whose witness searches
     * dominate runtime.
     */
    private void contractCellIntraParallel(int[] cellNodes, int[] order,
                                            ForkJoinPool pool,
                                            ThreadLocal<WitnessContext> tlCtx) {
        for (int node : cellNodes) {
            // Count active in-degree to decide parallelism strategy
            int[] iArr = inEdges[node];
            int iLen = Math.min(this.inLen[node], iArr.length);  // clamp: unsynchronized read
            int activeInDeg = 0;
            for (int j = 0; j < iLen; j++) {
                int u = buildEdgeData[iArr[j] * BE_SIZE + BE_FROM];
                if (!contracted[u] && u != node) activeInDeg++;
            }

            if (activeInDeg >= INTRA_PAR_MIN_IN_DEGREE) {
                contractNodeIntraParallel(node, pool, tlCtx);
            } else {
                contractNodeBatched(node);
            }
            contracted[node] = true;
            nodeLevel[node]  = order[node];
        }
    }

    /**
     * A small record holding the shortcuts discovered by one in-neighbour's
     * witness search.  Each parallel task creates its own instance, avoiding
     * the need for a single shared array whose size (inDeg × outDeg) can
     * overflow {@code int} on large networks.
     */
    private record InNbrShortcuts(int[] from, int[] to, int[] mid,
                                  int[] low1, int[] low2, double[] cost, int count) { }

    /**
     * Contract a single node with witness searches parallelized across
     * in-neighbours.  Each in-neighbour's witness search is independent
     * (it explores the graph from u, avoiding node v, to find witnesses
     * for all out-neighbours w), so they can safely run concurrently.
     *
     * <p>Each parallel task uses a {@code ThreadLocal} WitnessContext and
     * writes results into its own small per-in-neighbour result arrays,
     * avoiding the integer-overflow risk of a single combined array.
     * After all tasks complete, shortcuts are added in one synchronized batch.
     */
    private void contractNodeIntraParallel(int node, ForkJoinPool pool,
                                            ThreadLocal<WitnessContext> tlCtx) {
        int[] oArr = outEdges[node];
        int oLen = Math.min(outLen[node], oArr.length);  // clamp: unsynchronized read
        int[] iArr = inEdges[node];
        int iLen = Math.min(inLen[node], iArr.length);   // clamp: unsynchronized read

        // Collect active out-neighbors (targets)
        int numTargets = 0;
        int[] targets  = new int[oLen];
        double[] tgtCosts = new double[oLen];
        int[] tgtEdge  = new int[oLen];
        for (int i = 0; i < oLen; i++) {
            int outIdx = oArr[i];
            int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
            if (contracted[w] || w == node) continue;
            targets[numTargets]  = w;
            tgtCosts[numTargets] = buildEdgeWeights[outIdx];
            tgtEdge[numTargets]  = outIdx;
            numTargets++;
        }
        if (numTargets == 0) return;

        // Collect active in-neighbors
        int numInNbrs = 0;
        int[] inNbrs  = new int[iLen];
        int[] inIdxArr = new int[iLen];
        for (int j = 0; j < iLen; j++) {
            int inIdx = iArr[j];
            int u = buildEdgeData[inIdx * BE_SIZE + BE_FROM];
            if (contracted[u] || u == node) continue;
            inNbrs[numInNbrs]  = u;
            inIdxArr[numInNbrs] = inIdx;
            numInNbrs++;
        }
        if (numInNbrs == 0) return;

        final int nt = numTargets;
        final int ni = numInNbrs;

        // Each task produces its own small result — no shared array needed
        @SuppressWarnings("unchecked")
        ForkJoinTask<InNbrShortcuts>[] tasks = new ForkJoinTask[ni];
        for (int idx = 0; idx < ni; idx++) {
            final int u = inNbrs[idx];
            final int inIdx = inIdxArr[idx];

            tasks[idx] = pool.submit(() -> {
                WitnessContext ctx = tlCtx.get();
                double wUV = buildEdgeWeights[inIdx];

                // Compute max cost bound
                double globalMaxCost = 0;
                for (int t = 0; t < nt; t++) {
                    if (targets[t] == u) continue;
                    double bound = wUV + tgtCosts[t];
                    if (bound > globalMaxCost) globalMaxCost = bound;
                }

                // Run witness search
                batchedWitnessSearchCtx(u, node, globalMaxCost, HOP_LIMIT, SETTLED_LIMIT, ctx);

                // Dedup lookup
                ctx.dedupGeneration++;
                int[] uOutArr = outEdges[u];
                int uOLen = Math.min(outLen[u], uOutArr.length);  // clamp: unsynchronized read
                for (int k = 0; k < uOLen; k++) {
                    int ex = uOutArr[k];
                    int target = buildEdgeData[ex * BE_SIZE + BE_TO];
                    double eCost = buildEdgeWeights[ex];
                    if (ctx.dedupGen[target] != ctx.dedupGeneration || eCost < ctx.dedupBest[target]) {
                        ctx.dedupGen[target]  = ctx.dedupGeneration;
                        ctx.dedupBest[target] = eCost;
                    }
                }

                // Collect needed shortcuts for this in-neighbour
                int[] scF = new int[nt], scT = new int[nt], scM = new int[nt];
                int[] scL1 = new int[nt], scL2 = new int[nt];
                double[] scC = new double[nt];
                int count = 0;
                for (int t = 0; t < nt; t++) {
                    int w = targets[t];
                    if (w == u) continue;
                    double shortcutCost = wUV + tgtCosts[t];

                    double witCostW = (ctx.witnessIterIds[w] == ctx.witnessIteration)
                            ? ctx.witnessCost[w] : Double.POSITIVE_INFINITY;
                    if (witCostW <= shortcutCost) continue;

                    if (ctx.dedupGen[w] == ctx.dedupGeneration && ctx.dedupBest[w] <= shortcutCost) continue;

                    scF[count]  = u;
                    scT[count]  = w;
                    scM[count]  = node;
                    scL1[count] = inIdx;
                    scL2[count] = tgtEdge[t];
                    scC[count]  = shortcutCost;
                    count++;
                }
                return new InNbrShortcuts(scF, scT, scM, scL1, scL2, scC, count);
            });
        }

        // Wait for all witness searches to complete and add shortcuts
        for (ForkJoinTask<InNbrShortcuts> task : tasks) {
            InNbrShortcuts sc = task.join();
            for (int s = 0; s < sc.count; s++) {
                addBuildEdge(sc.from[s], sc.to[s], -1,
                        sc.mid[s], sc.low1[s], sc.low2[s], sc.cost[s]);
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
        int[] oArr = outEdges[node];
        int oLen = outLen[node];
        int[] iArr = inEdges[node];
        int iLen = inLen[node];

        // Collect active out-neighbours and their edge costs v→w.
        int numTargets = 0;
        for (int i = 0; i < oLen; i++) {
            int outIdx = oArr[i];
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
            int inIdx = iArr[j];
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
        int[] oArr = outEdges[node];
        int oLen = outLen[node];
        int[] iArr = inEdges[node];
        int iLen = inLen[node];

        // Collect active out-neighbors and their costs through node.
        int numTargets = 0;
        for (int i = 0; i < oLen; i++) {
            int outIdx = oArr[i];
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
            int inIdx = iArr[j];
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
            int[] uOutArr = outEdges[u];
            int uOLen = outLen[u];
            for (int k = 0; k < uOLen; k++) {
                int ex = uOutArr[k];
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

            int[] vOutArr = outEdges[v];
            int vOLen = outLen[v];
            for (int i = 0; i < vOLen; i++) {
                int edgeIdx = vOutArr[i];
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

    // ---- Context-aware versions for parallel contraction ----

    /**
     * Like {@link #contractNodeBatched(int)} but uses a thread-local
     * {@link WitnessContext} for witness search and collects all shortcuts
     * in a buffer, then adds them in one synchronized batch.
     *
     * <p>This reduces lock acquisitions from O(shortcuts_per_node) to O(1)
     * per contracted node, dramatically reducing contention.
     */
    private void contractNodeBatchedCtx(int node, WitnessContext ctx) {
        int[] oArr = outEdges[node];
        int oLen = Math.min(outLen[node], oArr.length);  // clamp: unsynchronized read
        int[] iArr = inEdges[node];
        int iLen = Math.min(inLen[node], iArr.length);   // clamp: unsynchronized read

        int numTargets = 0;
        for (int i = 0; i < oLen; i++) {
            int outIdx = oArr[i];
            int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
            if (contracted[w] || w == node) continue;
            if (numTargets >= ctx.scratchTargets.length) ctx.growScratch();
            ctx.scratchTargets[numTargets]    = w;
            ctx.scratchMaxCosts[numTargets]   = buildEdgeWeights[outIdx];
            ctx.scratchOutEdgeIdx[numTargets] = outIdx;
            numTargets++;
        }
        if (numTargets == 0) return;

        // Phase 1: Read-only — run witness searches and collect shortcuts
        ctx.scCount = 0;

        for (int j = 0; j < iLen; j++) {
            int inIdx = iArr[j];
            int u = buildEdgeData[inIdx * BE_SIZE + BE_FROM];
            if (contracted[u] || u == node) continue;
            double wUV = buildEdgeWeights[inIdx];

            double globalMaxCost = 0;
            for (int t = 0; t < numTargets; t++) {
                if (ctx.scratchTargets[t] == u) continue;
                double bound = wUV + ctx.scratchMaxCosts[t];
                if (bound > globalMaxCost) globalMaxCost = bound;
            }

            batchedWitnessSearchCtx(u, node, globalMaxCost, HOP_LIMIT, SETTLED_LIMIT, ctx);

            // Dedup lookup (read shared state — stale reads are conservative)
            ctx.dedupGeneration++;
            int[] uOutArr = outEdges[u];
            int uOLen = Math.min(outLen[u], uOutArr.length);  // clamp: unsynchronized read
            for (int k = 0; k < uOLen; k++) {
                int ex = uOutArr[k];
                int target = buildEdgeData[ex * BE_SIZE + BE_TO];
                double eCost = buildEdgeWeights[ex];
                if (ctx.dedupGen[target] != ctx.dedupGeneration || eCost < ctx.dedupBest[target]) {
                    ctx.dedupGen[target]  = ctx.dedupGeneration;
                    ctx.dedupBest[target] = eCost;
                }
            }

            for (int t = 0; t < numTargets; t++) {
                int w = ctx.scratchTargets[t];
                if (w == u) continue;
                double shortcutCost = wUV + ctx.scratchMaxCosts[t];

                double witnessCostToW = (ctx.witnessIterIds[w] == ctx.witnessIteration)
                        ? ctx.witnessCost[w] : Double.POSITIVE_INFINITY;
                if (witnessCostToW <= shortcutCost) continue;

                if (ctx.dedupGen[w] == ctx.dedupGeneration && ctx.dedupBest[w] <= shortcutCost) continue;

                ctx.addShortcut(u, w, node, inIdx, ctx.scratchOutEdgeIdx[t], shortcutCost);
            }
        }

        // Phase 2: Write — add shortcuts (addBuildEdge is self-synchronizing)
        if (ctx.scCount > 0) {
            for (int s = 0; s < ctx.scCount; s++) {
                addBuildEdge(ctx.scFrom[s], ctx.scTo[s], -1,
                        ctx.scMid[s], ctx.scLow1[s], ctx.scLow2[s], ctx.scCost[s]);
            }
        }
    }

    /**
     * Like {@link #batchedWitnessSearch} but uses a thread-local context.
     * Reads of the shared adjacency lists are unsynchronized — stale reads
     * are conservative (miss witnesses, producing slightly more shortcuts).
     */
    private void batchedWitnessSearchCtx(int source, int avoidNode, double maxCost,
                                          int hopLimit, int settledLimit,
                                          WitnessContext ctx) {
        ctx.witnessIteration++;
        ctx.witnessHeap.clear();

        ctx.witnessIterIds[source] = ctx.witnessIteration;
        ctx.witnessCost[source]    = 0.0;
        ctx.witnessHops[source]    = 0;
        ctx.witnessHeap.insert(source, 0.0);

        int settled = 0;
        while (!ctx.witnessHeap.isEmpty()) {
            int    v    = ctx.witnessHeap.poll();
            double cost = ctx.witnessCost[v];
            int    hops = ctx.witnessHops[v];

            if (cost > maxCost)   break;
            if (hops >= hopLimit) continue;
            if (++settled > settledLimit) break;

            int[] vOutArr = outEdges[v];
            int vOLen = Math.min(outLen[v], vOutArr.length);  // clamp: unsynchronized read
            for (int i = 0; i < vOLen; i++) {
                int edgeIdx = vOutArr[i];
                int toNode = buildEdgeData[edgeIdx * BE_SIZE + BE_TO];
                if (toNode == avoidNode) continue;
                if (contracted[toNode]) continue;

                double newCost = cost + buildEdgeWeights[edgeIdx];
                if (newCost > maxCost) continue;

                if (ctx.witnessIterIds[toNode] != ctx.witnessIteration) {
                    ctx.witnessCost[toNode]    = newCost;
                    ctx.witnessHops[toNode]    = hops + 1;
                    ctx.witnessIterIds[toNode] = ctx.witnessIteration;
                    ctx.witnessHeap.insert(toNode, newCost);
                } else if (newCost < ctx.witnessCost[toNode]) {
                    ctx.witnessCost[toNode]    = newCost;
                    ctx.witnessHops[toNode]    = hops + 1;
                    ctx.witnessHeap.decreaseKey(toNode, newCost);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3 – build SpeedyCHGraph (CSR layout)
    // -------------------------------------------------------------------------

    private SpeedyCHGraph buildCHGraph() {
        final int buildEdgeCount = buildEdgeCounter.get();

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
        int idx = buildEdgeCounter.getAndIncrement();
        int base = idx * BE_SIZE;
        // Grow if needed (rare with pre-sizing)
        if (base + BE_SIZE > buildEdgeData.length) {
            growBuildEdgeStorage(base + BE_SIZE);
        }
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

    /** Grow buildEdgeData/buildEdgeWeights under a lock (rare path). */
    private synchronized void growBuildEdgeStorage(int minDataLen) {
        if (minDataLen <= buildEdgeData.length) return; // another thread grew it
        int newLen = Math.max(buildEdgeData.length * 2, minDataLen);
        buildEdgeData    = Arrays.copyOf(buildEdgeData, newLen);
        buildEdgeWeights = Arrays.copyOf(buildEdgeWeights, newLen / BE_SIZE);
    }

}
