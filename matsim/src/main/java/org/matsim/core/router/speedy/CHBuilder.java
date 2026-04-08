/* *********************************************************************** *
 * project: org.matsim.*
 * CHBuilder.java
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2025 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */
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
 * Builds a {@link CHGraph} from a {@link SpeedyGraph} using the
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
 * @author Steffen Axer
 */
public class CHBuilder {

    private static final Logger LOG = LogManager.getLogger(CHBuilder.class);

    /**
     * Lightweight statistics collected during the CH build, used for concise
     * summary logging in the caller ({@link CHRouterFactory}).
     */
    public record BuildStats(
            long initEdgesNanos,
            long contractionNanos,
            long overlayBuildNanos,
            int baseEdges,
            int totalEdges,
            int deferredNodes,
            long deferredNanos
    ) {
        /** Shortcuts added during contraction. */
        public int shortcuts() { return totalEdges - baseEdges; }
    }

    /** Statistics from the last build invocation. */
    private BuildStats lastBuildStats;

    /** Returns the build statistics collected during the most recent build. */
    public BuildStats getLastBuildStats() { return lastBuildStats; }

    // Build-edge field indices (parallel arrays)
    private static final int BE_FROM  = 0;
    private static final int BE_TO    = 1;
    private static final int BE_ORIG  = 2; // originalLinkIndex; -1 = shortcut
    private static final int BE_MID   = 3; // middleNode;        -1 = real edge
    private static final int BE_LOW1  = 4; // lowerEdge1 build index; -1 = real edge
    private static final int BE_LOW2  = 5; // lowerEdge2 build index; -1 = real edge
    private static final int BE_SIZE  = 6;

    /** Maximum hops allowed in the witness search.
     *
     *  <p>On large networks (e.g. 500k+ nodes with transit overlays), a low hop limit
     *  prevents the witness search from finding existing alternative paths, causing
     *  unnecessary shortcuts that cascade into exponential edge/memory explosion.
     *  The settled limit ({@link #SETTLED_LIMIT}) is the primary search bound;
     *  the hop limit is a secondary safeguard. */
    private static final int HOP_LIMIT = 500;

    /** Base maximum number of nodes settled in a single witness search.
     *  The actual limit is scaled up with the number of active targets
     *  to prevent unnecessary shortcuts on large networks where separator
     *  nodes have high degree.
     *  @see #effectiveSettledLimit(int) */
    private static final int SETTLED_LIMIT = 5_000;

    /** Maximum settled limit after scaling.  Prevents unbounded exploration
     *  on degenerate networks while allowing enough room for large separators.
     *  Set to 100k: a good balance between witness-search thoroughness
     *  (preventing unnecessary shortcuts / edge explosion) and speed. */
    private static final int MAX_SETTLED_LIMIT = 100_000;

    /** Maximum settled limit during the deferred phase.  Deferred nodes have
     *  very few active neighbours (most of the graph is already contracted),
     *  so the effective graph is small.  A lower cap reduces witness search
     *  from O(thousands) to O(hundreds) per in-neighbour without meaningfully
     *  increasing shortcut count. */
    private static final int DEFERRED_MAX_SETTLED_LIMIT = 30_000;

    /** Active degree-product threshold below which the witness search is
     *  skipped entirely during the deferred phase.  When in×out ≤ this value,
     *  ALL u→w shortcuts are emitted unconditionally.  The witness search
     *  overhead dominates for small products, and the extra shortcuts are
     *  harmless (CH correctness is conservative). */
    private static final int SKIP_WITNESS_DEGREE_PRODUCT = 25;

    /** Cheaper hop/settled limits for priority estimation witness search.
     *
     *  <p>Higher values give more accurate priority estimates at the cost of
     *  slower preprocessing.  On large networks (500k+ nodes), accurate estimates
     *  are critical: inaccurate priorities cause high-degree hubs to be contracted
     *  too early, cascading unnecessary shortcuts.  A hop limit of 20 and
     *  settled limit of 1000 provide a good balance between accuracy and speed
     *  while being sufficient for shortcut-dense graphs after partial contraction. */
    private static final int PRIO_HOP_LIMIT = 20;
    private static final int PRIO_SETTLED_LIMIT = 1_000;

    /** Higher priority-estimation limits for the deferred PQ phase.
     *  Deferred nodes are high-degree hubs; inaccurate priority estimates
     *  cause excessive lazy-update re-insertions (each doubling witness-search
     *  work).  Higher limits pay for themselves by reducing re-insertions. */
    private static final int DEFERRED_PRIO_HOP_LIMIT = 50;
    private static final int DEFERRED_PRIO_SETTLED_LIMIT = 3_000;

    /** Minimum cell size for priority-based reordering within a cell.
     *  Cells with fewer nodes are contracted in their given order because
     *  the overhead of priority estimation outweighs the benefit.
     *
     *  <p>On large networks with PT overlays, separator cells at certain ND
     *  depths can contain thousands of high-degree hub nodes.  Without
     *  reordering, contracting these hubs in arbitrary order cascades edge
     *  inflation (observed: 1.2M → 4M edges in one round on the 532k-node
     *  Metropole Ruhr network).  Reordering by edge-difference priority
     *  contracts low-degree nodes first; their shortcuts then serve as
     *  witnesses for the high-degree hubs, drastically reducing the final
     *  edge count. */
    private static final int CELL_REORDER_THRESHOLD = 50;

    /** Cell size threshold for adaptive contraction.
     *  Cells with at least this many nodes use iterative priority-queue-based
     *  contraction where priorities are re-estimated after each contraction.
     *  This is critical for separator cells on 500k+ node networks:
     *  a one-shot priority sort cannot predict the cascading effect of
     *  shortcuts created by earlier contractions within the same cell.
     *  Iterative re-estimation ensures each node is contracted at the optimal
     *  time, using shortcuts from earlier contractions as witnesses.
     *
     *  <p>Lowered from 200 to match CELL_REORDER_THRESHOLD: any cell large
     *  enough to benefit from priority-aware ordering also benefits from
     *  the more accurate iterative re-estimation approach. */
    private static final int ADAPTIVE_CONTRACTION_THRESHOLD = 50;

    /** Maximum active degree product (active_in_degree × active_out_degree) for
     *  a node to be contracted during ND rounds.  Nodes exceeding this threshold
     *  are <em>deferred</em> to a final priority-queue (PQ) phase.
     *
     *  <p>On transit-augmented networks (e.g. Metropole Ruhr: 532k nodes, 1.16M
     *  links), PT hub nodes at ND separator depths have active degree products
     *  of 2000+, creating up to O(inDeg × outDeg) shortcuts per contraction.
     *  With thousands of such hubs in a single ND round, this produces millions
     *  of shortcuts and exhausts available memory (observed: 1.2M → 4M edges at
     *  ND depth 18, OOM with 20 GB heap).  Deferring these hubs to the PQ phase
     *  lets them be contracted <em>after</em> most of their neighbors, when the
     *  active degree is drastically lower.
     *
     *  <p>Set to 1000: balances between deferring enough to avoid edge explosion
     *  during ND rounds and keeping the deferred count low enough for fast PQ
     *  contraction.  At 500 the deferred count was ~25k on 750k-node networks,
     *  making the sequential PQ phase the dominant bottleneck.  At 1000 the
     *  deferred count is ~11k, which is 2× faster in the PQ phase. */
    private static final int DEFER_DEGREE_PRODUCT = 1000;

    /** Number of lock stripes for adjacency-list synchronization.
     *  Must be a power of two for fast modulo via bitmask.  8192 stripes provide
     *  low contention even with 32+ threads while saving memory vs per-node locks
     *  on 500k+ node networks. */
    private static final int ADJ_LOCK_STRIPES = 8192;

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
    private final Object[] adjLocks;  // striped locks for adjacency list mutation

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

    // Target membership for early termination in shared-state witness searches.
    private int        targetGeneration = 0;
    private final int[]    targetGen;

    // Scratch arrays reused across batched witness searches
    private int[]    scratchTargets;     // out-neighbor node indices
    private double[] scratchMaxCosts;    // max cost u→v→w for each target
    private int[]    scratchOutEdgeIdx;  // the out-edge index v→w for each target

    // Cell membership tracking for adaptive contraction (generation-stamped).
    // cellMemberGen[node] == cellGeneration means node is in the current cell.
    // AtomicInteger for thread-safe parallel adaptive cell processing.
    private final AtomicInteger cellGeneration = new AtomicInteger(0);
    private final int[] cellMemberGen;

    // Guard for reestimateCellNeighborsParallel: prevents duplicate processing
    // of the same node (reachable via both out-edges and in-edges).
    // nbrRemovedGen[node] == nbrRemovedGeneration means node was already visited
    // during the current reestimate call.
    // AtomicInteger for thread-safe parallel adaptive cell processing.
    private final AtomicInteger nbrRemovedGeneration = new AtomicInteger(0);
    private final int[] nbrRemovedGen;

    // Deferred-node tracking for BuildStats (written by contractNodesParallel)
    private int  lastDeferredCount;
    private long lastDeferredNanos;

    /** Set to {@code true} during the deferred PQ phase.  Used to activate
     *  more aggressive witness-search limits and skip-witness fast paths. */
    private volatile boolean deferredPhaseActive;

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

        // Target membership for early termination: targetGen[node] == targetGeneration
        // means node is one of the current witness-search targets.
        int targetGeneration = 0;
        final int[] targetGen;

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
            this.targetGen = new int[nodeCount];
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

    public CHBuilder(SpeedyGraph graph, TravelDisutility td) {
        this.graph     = graph;
        this.td        = td;
        this.nodeCount = graph.nodeCount;

        this.nodeLevel                = new int[nodeCount];
        this.contracted               = new boolean[nodeCount];
        this.contractedNeighborCount  = new int[nodeCount];

        // Pre-size to avoid reallocation during contraction.
        // ND ordering with deferred hubs typically produces 2.5-3.5× the original edges.
        int edgeCap = Math.max(graph.linkCount * 4, 16);
        this.buildEdgeData    = new int[edgeCap * BE_SIZE];
        this.buildEdgeWeights = new double[edgeCap];

        // Per-node adjacency lists – small initial capacity per node
        int perNode = Math.max(4, (graph.linkCount / Math.max(nodeCount, 1)) * 2 + 2);
        this.outEdges = new int[nodeCount][];
        this.outLen   = new int[nodeCount];
        this.inEdges  = new int[nodeCount][];
        this.inLen    = new int[nodeCount];
        this.adjLocks = new Object[ADJ_LOCK_STRIPES];
        for (int i = 0; i < ADJ_LOCK_STRIPES; i++) {
            adjLocks[i] = new Object();
        }
        for (int i = 0; i < nodeCount; i++) {
            outEdges[i] = new int[perNode];
            inEdges[i]  = new int[perNode];
        }

        this.witnessIterIds = new int[nodeCount];
        this.witnessCost    = new double[nodeCount];
        this.witnessHops    = new int[nodeCount];
        this.witnessHeap    = new DAryMinHeap(nodeCount, 4);

        this.dedupGen  = new int[nodeCount];
        this.dedupBest = new double[nodeCount];
        this.targetGen = new int[nodeCount];

        this.scratchTargets    = new int[64];
        this.scratchMaxCosts   = new double[64];
        this.scratchOutEdgeIdx = new int[64];

        // Cell membership tracking for adaptive contraction
        this.cellMemberGen = new int[nodeCount];

        // Guard against duplicate neighbor processing in reestimateCellNeighborsParallel
        this.nbrRemovedGen = new int[nodeCount];
    }

    /** Runs the full CH build pipeline and returns the ready-to-customize graph. */
    public CHGraph build() {
        int baseEdges = graph.linkCount;
        LOG.debug("CH contraction: importing {} links from base graph ({} nodes)…",
                baseEdges, nodeCount);
        long t0 = System.nanoTime();
        initEdges();
        long t1 = System.nanoTime();

        LOG.debug("CH contraction: contracting {} nodes…", nodeCount);
        contractNodes();
        long t2 = System.nanoTime();

        LOG.debug("CH contraction: building overlay graph ({} edges)…", buildEdgeCounter.get());
        CHGraph result = buildCHGraph();
        long t3 = System.nanoTime();

        this.lastBuildStats = new BuildStats(t1 - t0, t2 - t1, t3 - t2,
                baseEdges, result.totalEdgeCount, 0, 0);
        return result;
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
    public CHGraph buildWithOrder(int[] order) {
        int baseEdges = graph.linkCount;
        LOG.debug("CH contraction (fixed order): importing {} links from base graph ({} nodes)…",
                baseEdges, nodeCount);
        long t0 = System.nanoTime();
        initEdges();
        long t1 = System.nanoTime();

        LOG.debug("CH contraction (fixed order): contracting {} nodes…", nodeCount);
        contractNodesInOrder(order);
        long t2 = System.nanoTime();

        LOG.debug("CH contraction (fixed order): building overlay graph ({} edges)…", buildEdgeCounter.get());
        CHGraph result = buildCHGraph();
        long t3 = System.nanoTime();

        this.lastBuildStats = new BuildStats(t1 - t0, t2 - t1, t3 - t2,
                baseEdges, result.totalEdgeCount, 0, 0);
        return result;
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
    public CHGraph buildWithOrderParallel(InertialFlowCutter.NDOrderResult orderResult) {
        int baseEdges = graph.linkCount;
        LOG.debug("CH contraction (parallel): importing {} links from base graph ({} nodes)…",
                baseEdges, nodeCount);
        long t0 = System.nanoTime();
        initEdges();
        long t1 = System.nanoTime();

        int nThreads = Runtime.getRuntime().availableProcessors();
        LOG.debug("CH contraction (parallel): contracting {} nodes with {} rounds, {} threads…",
                nodeCount, orderResult.rounds.size(), nThreads);
        contractNodesParallel(orderResult.order, orderResult.rounds, nThreads);
        long t2 = System.nanoTime();

        LOG.debug("CH contraction (parallel): building overlay graph ({} edges)…", buildEdgeCounter.get());
        CHGraph result = buildCHGraph();
        long t3 = System.nanoTime();

        this.lastBuildStats = new BuildStats(t1 - t0, t2 - t1, t3 - t2,
                baseEdges, result.totalEdgeCount, lastDeferredCount, lastDeferredNanos);
        return result;
    }

    // ---- per-node adjacency helpers ----

    private void adjOutAdd(int node, int edgeIdx) {
        synchronized (adjLocks[node & (ADJ_LOCK_STRIPES - 1)]) {
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
        synchronized (adjLocks[node & (ADJ_LOCK_STRIPES - 1)]) {
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
                LOG.debug("  … contracted {}/{} nodes ({} edges so far)",
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
                LOG.debug("  … contracted {}/{} nodes ({} edges so far)",
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
     *   <li><b>Inter-cell parallelism</b> – cells within the same ND round are
     *       guaranteed independent (no edges between them, separators contracted
     *       in later rounds), so they can safely be contracted concurrently.
     *       Each cell uses a thread-local {@link WitnessContext} for its witness
     *       searches.  {@code addBuildEdge} is already thread-safe (striped locks).
     *       Stale reads of adjacency lists during concurrent witness searches are
     *       conservative (may miss witnesses → more shortcuts, never incorrect).</li>
     *   <li><b>Intra-node parallelism</b> – for shallow rounds with few but large
     *       cells (especially the root separator), the independent witness searches
     *       within each node contraction are parallelized across in-neighbours.</li>
     *   <li><b>Batched edge addition</b> – shortcuts are collected in thread-local
     *       buffers and flushed under one lock per contracted node.</li>
     * </ul>
     */
    private void contractNodesParallel(int[] order, List<List<int[]>> rounds, int nThreads) {
        ForkJoinPool pool = new ForkJoinPool(nThreads);

        try {
            ThreadLocal<WitnessContext> tlCtx = ThreadLocal.withInitial(() -> new WitnessContext(nodeCount));

            int totalContracted = 0;

            for (int r = 0; r < rounds.size(); r++) {
                List<int[]> round = rounds.get(r);
                int roundNodes = roundNodeCount(round);

                // Compact adjacency lists before late ND rounds.
                // When >90% of nodes are contracted, the remaining nodes'
                // adjacency lists are bloated with stale entries to contracted
                // nodes.  Compacting before the round eliminates ~90%+ of
                // per-iteration overhead in witness searches.
                if (totalContracted > nodeCount * 0.90 && roundNodes > 0) {
                    int[] roundNodesArr = new int[roundNodes];
                    int pos = 0;
                    for (int[] cell : round) {
                        for (int node : cell) {
                            roundNodesArr[pos++] = node;
                        }
                    }
                    compactAdjacencyLists(roundNodesArr);
                }

                // Separate cells into adaptive (large) and standard (small).
                List<int[]> adaptiveCells = new ArrayList<>();
                List<int[]> standardCells = new ArrayList<>();
                for (int[] cell : round) {
                    if (cell.length >= ADAPTIVE_CONTRACTION_THRESHOLD) {
                        adaptiveCells.add(cell);
                    } else {
                        standardCells.add(cell);
                    }
                }

                // ---- Phase A: Contract large cells adaptively ----
                // Large cells use iterative PQ-based contraction with priority
                // re-estimation.  Contracted sequentially because each cell
                // uses intra-node parallelism (nested pool.submit) which
                // would risk deadlocks if the cell itself ran inside the pool.
                for (int[] cell : adaptiveCells) {
                    contractCellAdaptive(cell, order, pool, tlCtx);
                }

                // ---- Phase B: Contract standard (small) cells in parallel ----
                // Reorder nodes within each cell by estimated priority first.
                reorderCellsByPriority(standardCells, order, pool, tlCtx);

                // Inter-cell parallelism: submit independent cells to the pool.
                // Merge very small cells into chunks for better load balancing.
                List<int[]> chunks = mergeSmallCells(standardCells, 500);

                if (chunks.size() > 1 && pool != null) {
                    // True inter-cell parallelism: each chunk runs on its own thread
                    // with a thread-local WitnessContext.  Cells within the same ND
                    // round are guaranteed independent (no edges between them).
                    // For shallow rounds with few but large chunks, we also enable
                    // intra-node parallelism within each chunk (hybrid approach).
                    boolean fewChunks = chunks.size() <= pool.getParallelism();
                    List<ForkJoinTask<?>> tasks = new ArrayList<>(chunks.size());
                    for (int[] chunk : chunks) {
                        if (fewChunks) {
                            // Few large chunks: use intra-node parallelism within each
                            tasks.add(pool.submit(() ->
                                    contractCellIntraParallel(chunk, order, pool, tlCtx)));
                        } else {
                            // Many small chunks: pure inter-cell parallelism
                            tasks.add(pool.submit(() -> {
                                WitnessContext ctx = tlCtx.get();
                                contractCellWithContext(chunk, order, ctx);
                            }));
                        }
                    }
                    for (ForkJoinTask<?> t : tasks) t.join();
                } else if (!chunks.isEmpty()) {
                    // Single chunk or no pool: use intra-node parallelism
                    for (int[] chunk : chunks) {
                        contractCellIntraParallel(chunk, order, pool, tlCtx);
                    }
                }

                totalContracted += roundNodes;
                LOG.debug("  … contracted {}/{} nodes ({}%), depth {}/{} ({} edges so far)",
                        totalContracted, nodeCount,
                        (int) (100.0 * totalContracted / nodeCount),
                        r + 1, rounds.size(), buildEdgeCounter.get());
            }

            // Contract deferred high-degree nodes.
            int deferredCount = 0;
            for (int n = 0; n < nodeCount; n++) {
                if (!contracted[n]) deferredCount++;
            }
            this.lastDeferredCount = deferredCount;
            if (deferredCount > 0) {
                LOG.debug("CH contraction (parallel): contracting {} deferred high-degree nodes with PQ…",
                        deferredCount);
                long deferStart = System.nanoTime();
                contractDeferredNodesPQ(deferredCount, pool, tlCtx);
                this.lastDeferredNanos = System.nanoTime() - deferStart;
            } else {
                this.lastDeferredNanos = 0;
            }

        } finally {
            pool.shutdown();
        }
    }

    /**
     * Contracts deferred high-degree nodes using a <b>sort-once, contract-in-order</b>
     * approach — no lazy priority-queue re-estimation.
     *
     * <h3>Key optimisations vs the previous lazy-PQ implementation</h3>
     * <ol>
     *   <li><b>No lazy re-estimation</b>: priorities are estimated once in parallel,
     *       nodes are sorted by ascending priority, and contracted in that order.
     *       This eliminates 50–70% of redundant witness-search work that was spent
     *       on re-estimating nodes only to re-insert them into the PQ.</li>
     *   <li><b>Dominated-edge pruning</b>: before contraction, redundant parallel
     *       edges (multiple edges u→w, keeping only the cheapest) are removed.
     *       Deferred hubs accumulate massive numbers of shortcut edges; pruning
     *       drastically reduces the effective degree and thus witness-search cost.</li>
     *   <li><b>Aggressive settled limits</b>: the deferred phase uses a lower
     *       settled limit ({@link #DEFERRED_MAX_SETTLED_LIMIT}) because the
     *       effective graph is tiny (most nodes already contracted).</li>
     *   <li><b>Skip witness search for tiny degree products</b>: when
     *       in×out ≤ {@link #SKIP_WITNESS_DEGREE_PRODUCT}, all shortcuts are
     *       emitted unconditionally, skipping the expensive witness search.</li>
     * </ol>
     *
     * <p>Levels for deferred nodes start at {@code nodeCount} to ensure they are
     * above all ND-assigned levels (which are in {@code [0, nodeCount-1]}).
     *
     * @param deferredCount number of uncontracted nodes (for logging).
     * @param pool          thread pool for intra-node parallelism (may be null).
     * @param tlCtx         thread-local witness contexts (may be null when pool is null).
     */
    private void contractDeferredNodesPQ(int deferredCount,
                                          ForkJoinPool pool,
                                          ThreadLocal<WitnessContext> tlCtx) {
        // ---- Step 0: Compact adjacency lists of deferred nodes ----
        int[] deferredNodes = new int[deferredCount];
        int di = 0;
        for (int node = 0; node < nodeCount; node++) {
            if (!contracted[node]) deferredNodes[di++] = node;
        }
        compactAdjacencyLists(deferredNodes);

        // ---- Step 0b: Prune dominated parallel edges ----
        // Deferred hubs have accumulated massive numbers of shortcuts.
        // Many edges u→w are dominated by a cheaper edge u→w.  Pruning
        // reduces the active degree (and thus numTargets in witness search).
        pruneParallelEdges(deferredNodes, deferredCount);

        // Initialize contractedNeighborCount for deferred nodes — needed for
        // accurate priority estimation.
        for (int i = 0; i < deferredCount; i++) {
            int node = deferredNodes[i];
            int count = 0;
            int[] oArr = outEdges[node];
            int oLen = this.outLen[node];
            for (int k = 0; k < oLen; k++) {
                int w = buildEdgeData[oArr[k] * BE_SIZE + BE_TO];
                if (contracted[w]) count++;
            }
            int[] iArr = inEdges[node];
            int iLen = this.inLen[node];
            for (int j = 0; j < iLen; j++) {
                int u = buildEdgeData[iArr[j] * BE_SIZE + BE_FROM];
                if (contracted[u]) count++;
            }
            contractedNeighborCount[node] = count;
        }

        // Phase 1: Parallel initial priority estimation.
        int[] nodePriority = new int[nodeCount];
        if (pool != null && deferredCount >= 16) {
            int batchSize = Math.max(1, deferredCount / (pool.getParallelism() * 4));
            List<ForkJoinTask<?>> prioTasks = new ArrayList<>();
            for (int from = 0; from < deferredCount; from += batchSize) {
                final int start = from;
                final int end = Math.min(from + batchSize, deferredCount);
                prioTasks.add(pool.submit(() -> {
                    WitnessContext ctx = tlCtx.get();
                    for (int i = start; i < end; i++) {
                        nodePriority[deferredNodes[i]] = estimatePriorityCtx(
                                deferredNodes[i], ctx,
                                DEFERRED_PRIO_HOP_LIMIT, DEFERRED_PRIO_SETTLED_LIMIT);
                    }
                }));
            }
            for (ForkJoinTask<?> t : prioTasks) t.join();
        } else {
            for (int i = 0; i < deferredCount; i++) {
                nodePriority[deferredNodes[i]] = estimatePriority(
                        deferredNodes[i], DEFERRED_PRIO_HOP_LIMIT, DEFERRED_PRIO_SETTLED_LIMIT);
            }
        }

        // Phase 2: Sort deferred nodes by ascending priority (one-shot).
        long[] sortPairs = new long[deferredCount];
        for (int i = 0; i < deferredCount; i++) {
            int node = deferredNodes[i];
            sortPairs[i] = ((long)(nodePriority[node] + nodeCount) << 32) | (node & 0xFFFFFFFFL);
        }
        Arrays.sort(sortPairs);

        // Rebuild deferredNodes in sorted order.
        for (int i = 0; i < deferredCount; i++) {
            deferredNodes[i] = (int) sortPairs[i];
        }

        // ---- Activate deferred-phase optimisations ----
        deferredPhaseActive = true;

        // Phase 3: Contract deferred nodes sequentially in sorted order.
        // No lazy-update, no independence batching — just iterate and contract.
        int levelCounter = nodeCount; // above all ND levels
        int logInterval = Math.max(deferredCount / 20, 1);
        int compactInterval = Math.max(deferredCount / 5, 500);

        for (int i = 0; i < deferredCount; i++) {
            int node = deferredNodes[i];
            if (contracted[node]) continue; // should not happen, but guard

            int activeInDeg = countActiveInDeg(node);
            int activeOutDeg = countActiveOutDeg(node);
            long degProduct = (long) activeInDeg * activeOutDeg;

            if (degProduct <= SKIP_WITNESS_DEGREE_PRODUCT) {
                // Fast path: emit all shortcuts without witness search.
                contractNodeNoWitness(node);
            } else if (pool != null && activeInDeg >= DEFERRED_INTRA_PAR_MIN_IN_DEGREE) {
                contractNodeIntraParallel(node, pool, tlCtx);
            } else if (pool != null) {
                WitnessContext ctx = tlCtx.get();
                contractNodeBatchedCtx(node, ctx);
            } else {
                contractNodeBatched(node);
            }
            contracted[node] = true;
            nodeLevel[node] = levelCounter++;

            if ((i + 1) % logInterval == 0) {
                LOG.debug("  … deferred: contracted {}/{} nodes ({} edges so far)",
                        i + 1, deferredCount, buildEdgeCounter.get());
            }

            // Periodic re-compaction: adjacency lists grow with new shortcuts.
            if ((i + 1) % compactInterval == 0 && i + 1 < deferredCount) {
                int remaining = deferredCount - i - 1;
                int[] remNodes = new int[remaining];
                int ri = 0;
                for (int j = i + 1; j < deferredCount; j++) {
                    if (!contracted[deferredNodes[j]]) remNodes[ri++] = deferredNodes[j];
                }
                if (ri > 0) {
                    compactAdjacencyListsDirect(remNodes, ri);
                    pruneParallelEdgesDirect(remNodes, ri);
                }
            }
        }

        deferredPhaseActive = false;

        LOG.debug("  … deferred: contracted {}/{} nodes ({} edges total)",
                deferredCount, deferredCount, buildEdgeCounter.get());

        // Remap all node levels to a contiguous permutation [0, nodeCount-1].
        remapLevels();
    }

    /**
     * Counts active (uncontracted, non-self) out-neighbours of a node.
     */
    private int countActiveOutDeg(int node) {
        int[] oArr = outEdges[node];
        int oLen = this.outLen[node];
        int deg = 0;
        for (int i = 0; i < oLen; i++) {
            int w = buildEdgeData[oArr[i] * BE_SIZE + BE_TO];
            if (!contracted[w] && w != node) deg++;
        }
        return deg;
    }

    /**
     * Contracts a node by emitting ALL shortcuts without running any witness
     * search.  Used when the active degree product is tiny
     * (in×out ≤ {@link #SKIP_WITNESS_DEGREE_PRODUCT}), where the witness
     * search overhead dominates and the few extra shortcuts are harmless.
     *
     * <p>Still performs parallel-edge deduplication to avoid trivially
     * redundant shortcuts.
     */
    private void contractNodeNoWitness(int node) {
        int[] oArr = outEdges[node];
        int oLen = Math.min(outLen[node], oArr.length);
        int[] iArr = inEdges[node];
        int iLen = Math.min(inLen[node], iArr.length);

        // Collect active out-neighbors
        int numTargets = 0;
        for (int i = 0; i < oLen; i++) {
            int outIdx = oArr[i];
            int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
            if (contracted[w] || w == node) continue;
            if (numTargets >= scratchTargets.length) growScratch();
            scratchTargets[numTargets]    = w;
            scratchMaxCosts[numTargets]   = buildEdgeWeights[outIdx];
            scratchOutEdgeIdx[numTargets] = outIdx;
            numTargets++;
        }
        if (numTargets == 0) return;

        for (int j = 0; j < iLen; j++) {
            int inIdx = iArr[j];
            int u = buildEdgeData[inIdx * BE_SIZE + BE_FROM];
            if (contracted[u] || u == node) continue;
            double wUV = buildEdgeWeights[inIdx];

            // Dedup lookup: best existing out-edge cost from u
            dedupGeneration++;
            int[] uOutArr = outEdges[u];
            int uOLen = Math.min(outLen[u], uOutArr.length);
            for (int k = 0; k < uOLen; k++) {
                int ex = uOutArr[k];
                int target = buildEdgeData[ex * BE_SIZE + BE_TO];
                double eCost = buildEdgeWeights[ex];
                if (dedupGen[target] != dedupGeneration || eCost < dedupBest[target]) {
                    dedupGen[target]  = dedupGeneration;
                    dedupBest[target] = eCost;
                }
            }

            for (int t = 0; t < numTargets; t++) {
                int w = scratchTargets[t];
                if (w == u) continue;
                double shortcutCost = wUV + scratchMaxCosts[t];
                // Only dedup check — no witness search.
                if (dedupGen[w] == dedupGeneration && dedupBest[w] <= shortcutCost) continue;
                addBuildEdge(u, w, -1, node, inIdx, scratchOutEdgeIdx[t], shortcutCost);
            }
        }
    }

    /**
     * Prunes dominated parallel edges from the adjacency lists of the given nodes.
     *
     * <p>After ND rounds, deferred hubs accumulate many shortcut edges to the same
     * target (multiple paths through different contracted nodes).  For out-edges,
     * if there are multiple edges u→w, only the cheapest is retained.  For in-edges,
     * if there are multiple edges u→v, only the cheapest is retained.
     *
     * <p>This reduces the active degree used in witness searches and dedup scans,
     * dramatically cutting witness search runtime for high-degree hubs.
     *
     * <p>Also prunes the active neighbours of the given nodes (1-hop frontier)
     * since witness searches explore those adjacency lists.
     */
    private void pruneParallelEdges(int[] nodes, int count) {
        // Mark nodes to prune (including their active neighbours)
        int gen = this.nbrRemovedGeneration.incrementAndGet();
        for (int i = 0; i < count; i++) {
            nbrRemovedGen[nodes[i]] = gen;
        }
        for (int i = 0; i < count; i++) {
            int node = nodes[i];
            int[] oArr = outEdges[node];
            int oLen = outLen[node];
            for (int k = 0; k < oLen; k++) {
                int w = buildEdgeData[oArr[k] * BE_SIZE + BE_TO];
                if (!contracted[w]) nbrRemovedGen[w] = gen;
            }
            int[] iArr = inEdges[node];
            int iLen = inLen[node];
            for (int j = 0; j < iLen; j++) {
                int u = buildEdgeData[iArr[j] * BE_SIZE + BE_FROM];
                if (!contracted[u]) nbrRemovedGen[u] = gen;
            }
        }

        for (int n = 0; n < nodeCount; n++) {
            if (nbrRemovedGen[n] != gen || contracted[n]) continue;
            pruneOutParallel(n);
            pruneInParallel(n);
        }
    }

    /**
     * Lightweight parallel-edge pruning: only the given nodes, no neighbour walk.
     */
    private void pruneParallelEdgesDirect(int[] nodes, int count) {
        for (int i = 0; i < count; i++) {
            int n = nodes[i];
            if (contracted[n]) continue;
            pruneOutParallel(n);
            pruneInParallel(n);
        }
    }

    /**
     * Prune out-edges of a node: for each target w, keep only the cheapest edge.
     * Uses the generic {@link #pruneKeepCheapest} helper.
     */
    private void pruneOutParallel(int node) {
        synchronized (adjLocks[node & (ADJ_LOCK_STRIPES - 1)]) {
            int[] arr = outEdges[node];
            int len = outLen[node];
            if (len <= 1) return;
            outLen[node] = pruneKeepCheapest(arr, len, true);
        }
    }

    private void pruneInParallel(int node) {
        synchronized (adjLocks[node & (ADJ_LOCK_STRIPES - 1)]) {
            int[] arr = inEdges[node];
            int len = inLen[node];
            if (len <= 1) return;
            inLen[node] = pruneKeepCheapest(arr, len, false);
        }
    }

    /**
     * Generic parallel-edge pruning helper.  Given an array of build-edge indices,
     * keeps only the cheapest edge per (neighbour node).  Returns the new length.
     *
     * @param isOut if true, the neighbour is BE_TO; if false, BE_FROM.
     */
    private int pruneKeepCheapest(int[] arr, int len, boolean isOut) {
        if (len <= 1) return len;
        int nbrField = isOut ? BE_TO : BE_FROM;

        // Pass 1: find cheapest cost per neighbour (using generation stamp)
        dedupGeneration++;
        int myGen = dedupGeneration;
        for (int i = 0; i < len; i++) {
            int edgeIdx = arr[i];
            int nbr = buildEdgeData[edgeIdx * BE_SIZE + nbrField];
            if (contracted[nbr]) continue;
            double cost = buildEdgeWeights[edgeIdx];
            if (dedupGen[nbr] != myGen || cost < dedupBest[nbr]) {
                dedupGen[nbr]  = myGen;
                dedupBest[nbr] = cost;
            }
        }

        // Pass 2: keep first edge per neighbour that matches the cheapest cost.
        // Use a second generation to track which neighbours have been emitted.
        dedupGeneration++;
        int emitGen = dedupGeneration;
        int write = 0;
        for (int i = 0; i < len; i++) {
            int edgeIdx = arr[i];
            int nbr = buildEdgeData[edgeIdx * BE_SIZE + nbrField];
            if (contracted[nbr]) continue; // skip stale
            if (dedupGen[nbr] == emitGen) continue; // already emitted
            double cost = buildEdgeWeights[edgeIdx];
            if (dedupGen[nbr] == myGen && cost == dedupBest[nbr]) {
                // This is the cheapest edge to this neighbour
                dedupGen[nbr] = emitGen; // mark as emitted
                arr[write++] = edgeIdx;
            }
        }
        // If some cheapest edges didn't match due to floating-point: keep remaining
        // neighbours that weren't emitted (take any edge).
        for (int i = 0; i < len; i++) {
            int edgeIdx = arr[i];
            int nbr = buildEdgeData[edgeIdx * BE_SIZE + nbrField];
            if (contracted[nbr]) continue;
            if (dedupGen[nbr] == emitGen) continue; // already emitted
            dedupGen[nbr] = emitGen;
            arr[write++] = edgeIdx;
        }
        return write;
    }

    /**
     * Remaps {@link #nodeLevel} values to form a contiguous permutation
     * {@code [0, nodeCount-1]}.  Preserves the relative order of all levels.
     *
     * <p>This is needed when deferred nodes are assigned levels >= nodeCount
     * to keep them above all ND levels during contraction.  CHGraph requires
     * levels to be exactly the set {0, 1, …, nodeCount-1}.
     */
    private void remapLevels() {
        // Pair each node with its current level, sort by level, then reassign.
        long[] pairs = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            pairs[i] = ((long) nodeLevel[i] << 32) | (i & 0xFFFFFFFFL);
        }
        Arrays.sort(pairs);
        for (int rank = 0; rank < nodeCount; rank++) {
            int node = (int) pairs[rank];
            nodeLevel[node] = rank;
        }
    }

    private static int roundNodeCount(List<int[]> round) {
        int count = 0;
        for (int[] cell : round) count += cell.length;
        return count;
    }

    /**
     * Reorders nodes within each original ND cell by estimated priority
     * (edge-difference heuristic) and updates the {@code order} array so
     * that the assigned ND ranks match the new contraction order.
     *
     * <p>This must be called <b>before</b> {@link #mergeSmallCells} to
     * ensure each cell's rank range stays intact.  After this call, each
     * cell's nodes are sorted by ascending priority (low-priority = low
     * edge-difference nodes first), and their ND ranks are reassigned
     * within the cell's original rank band.
     *
     * <p>On large networks with PT overlays, separator cells at certain ND
     * depths contain thousands of high-degree hub nodes.  Without
     * reordering, contracting these hubs in arbitrary order cascades edge
     * inflation.  Reordering by edge-difference contracts low-degree
     * nodes first; their shortcuts then serve as witnesses for the
     * high-degree hubs.
     *
     * <p>Uses either the builder's shared witness state (single-threaded) or
     * thread-local WitnessContexts for parallel execution across cells.
     */
    private void reorderCellsByPriority(List<int[]> cells, int[] order,
                                         ForkJoinPool pool,
                                         ThreadLocal<WitnessContext> tlCtx) {
        // Phase 0: Identify cells that need reordering and collect their rank bands.
        List<int[]> largeCells = new ArrayList<>();
        List<int[]> cellRanks = new ArrayList<>();
        for (int[] cell : cells) {
            if (cell.length < CELL_REORDER_THRESHOLD) continue;
            largeCells.add(cell);
            int n = cell.length;
            int[] ranks = new int[n];
            for (int i = 0; i < n; i++) ranks[i] = order[cell[i]];
            Arrays.sort(ranks);
            cellRanks.add(ranks);
        }

        if (largeCells.isEmpty()) return;

        // Phase 1: Sort cells by priority (parallel if multiple cells and pool available).
        if (pool != null && largeCells.size() > 1) {
            List<ForkJoinTask<?>> tasks = new ArrayList<>(largeCells.size());
            for (int[] cell : largeCells) {
                tasks.add(pool.submit(() -> {
                    WitnessContext ctx = tlCtx.get();
                    sortCellByPriorityCtx(cell, ctx);
                }));
            }
            for (ForkJoinTask<?> t : tasks) t.join();
        } else {
            for (int[] cell : largeCells) {
                sortCellByPriority(cell);
            }
        }

        // Phase 2: Reassign ranks (sequential, cheap).
        for (int k = 0; k < largeCells.size(); k++) {
            int[] cell = largeCells.get(k);
            int[] ranks = cellRanks.get(k);
            int n = cell.length;
            for (int i = 0; i < n; i++) {
                order[cell[i]] = ranks[i];
            }
        }
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

    /**
     * Checks whether a node should be deferred from ND contraction to the
     * final priority-queue phase.  Returns {@code true} if the product of
     * active (uncontracted, non-self) in-degree and out-degree exceeds
     * {@link #DEFER_DEGREE_PRODUCT}.
     *
     * <p>Uses early termination: as soon as the running product exceeds the
     * threshold while counting in-neighbours, the method returns immediately.
     */
    private boolean shouldDefer(int node) {
        int outDeg = 0;
        int[] oArr = outEdges[node];
        int oLen = this.outLen[node];
        for (int i = 0; i < oLen; i++) {
            int w = buildEdgeData[oArr[i] * BE_SIZE + BE_TO];
            if (!contracted[w] && w != node) outDeg++;
        }
        if (outDeg == 0) return false;

        int[] iArr = inEdges[node];
        int iLen = this.inLen[node];
        int inDeg = 0;
        for (int j = 0; j < iLen; j++) {
            int u = buildEdgeData[iArr[j] * BE_SIZE + BE_FROM];
            if (!contracted[u] && u != node) {
                if (++inDeg * (long) outDeg > DEFER_DEGREE_PRODUCT) return true;
            }
        }
        return false;
    }

    /** Contract a cell sequentially using the builder's own witness state. */
    private void contractCellSequential(int[] cellNodes, int[] order) {
        for (int node : cellNodes) {
            if (shouldDefer(node)) continue;
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
            if (shouldDefer(node)) continue;
            contractNodeBatchedCtx(node, ctx);
            contracted[node] = true;
            nodeLevel[node]  = order[node];
        }
    }

    /**
     * Adaptively contracts a large cell using iterative priority-queue-based ordering.
     *
     * <p>Instead of sorting all nodes by a one-shot priority estimate and then
     * contracting in that fixed order, this method:
     * <ol>
     *   <li>Estimates initial priorities for all nodes in the cell.</li>
     *   <li>Inserts them into a min-heap (lowest priority = contract first).</li>
     *   <li>Pops the minimum-priority node, contracts it.</li>
     *   <li>Re-estimates priorities of uncontracted neighbors within the same cell
     *       (because the shortcuts just created may serve as new witnesses, lowering
     *       their edge-difference).</li>
     *   <li>Assigns ranks to contracted nodes in the order they are actually contracted,
     *       using the cell's original rank band.</li>
     * </ol>
     *
     * <p>This prevents the cascading edge explosion observed on 500k+ node networks
     * (e.g. Metropole Ruhr): contracting a low-degree node creates shortcuts that
     * the witness search for high-degree neighbors can discover, avoiding unnecessary
     * additional shortcuts.
     *
     * <p>Uses intra-node parallelism for the witness searches when a pool is
     * available, and parallel re-estimation of cell neighbors after each
     * contraction.  The PQ ordering remains sequential (node selection),
     * but the expensive witness searches within each contraction are
     * distributed across threads.
     *
     * @param pool  thread pool for intra-node parallelism (may be null).
     * @param tlCtx thread-local witness contexts (may be null when pool is null).
     */
    private void contractCellAdaptive(int[] cellNodes, int[] order,
                                       ForkJoinPool pool,
                                       ThreadLocal<WitnessContext> tlCtx) {
        int n = cellNodes.length;

        // 1. Collect and sort the rank band for this cell.
        int[] ranks = new int[n];
        for (int i = 0; i < n; i++) ranks[i] = order[cellNodes[i]];
        Arrays.sort(ranks);

        // 2. Mark cell membership using generation stamp.
        int cellGen = this.cellGeneration.incrementAndGet();
        for (int node : cellNodes) cellMemberGen[node] = cellGen;

        // 3. Estimate initial priorities and insert into PQ.
        //    Track stored priorities for lazy update comparison.
        //    We maintain a boolean[] inPQ to track PQ membership
        //    explicitly, because DAryMinHeap.remove() does not
        //    reset pos[node] after removal (unlike poll()).
        DAryMinHeap pq = new DAryMinHeap(nodeCount, 4);
        int[] storedPrio = new int[nodeCount];
        boolean[] inPQ = new boolean[nodeCount];

        // Parallel initial priority estimation when pool is available and cell is large enough.
        // Each estimation is read-only on the graph structure (no nodes contracted yet in this cell).
        if (pool != null && n >= 16) {
            int[] prios = new int[n];
            int batchSize = Math.max(1, n / (pool.getParallelism() * 4));
            List<ForkJoinTask<?>> prioTasks = new ArrayList<>();
            for (int from = 0; from < n; from += batchSize) {
                final int start = from;
                final int end = Math.min(from + batchSize, n);
                prioTasks.add(pool.submit(() -> {
                    WitnessContext ctx = tlCtx.get();
                    for (int i = start; i < end; i++) {
                        prios[i] = estimatePriorityCtx(cellNodes[i], ctx);
                    }
                }));
            }
            for (ForkJoinTask<?> t : prioTasks) t.join();

            for (int i = 0; i < n; i++) {
                int node = cellNodes[i];
                storedPrio[node] = prios[i];
                pq.insert(node, prios[i] + nodeCount);
                inPQ[node] = true;
            }
        } else {
            for (int node : cellNodes) {
                int prio = estimatePriority(node);
                storedPrio[node] = prio;
                // Shift priority to non-negative range for the heap (edge-diff can be negative).
                // Add nodeCount to ensure all values are positive.
                pq.insert(node, prio + nodeCount);
                inPQ[node] = true;
            }
        }

        // 4. Iterative contraction with lazy priority update.
        //    When a node is popped, re-estimate its priority.  If it increased
        //    (i.e. contracting it now would create more shortcuts than expected),
        //    re-insert it and try the next node.  This prevents premature
        //    contraction of high-degree hubs whose true priority rises as
        //    the graph structure changes from earlier contractions.
        int contractionIdx = 0;
        while (!pq.isEmpty()) {
            int node = pq.poll();
            inPQ[node] = false;

            // Lazy update: re-estimate and re-queue if priority increased.
            // Use ctx-based estimation when pool is available (thread-safe for parallel cells).
            int newPrio;
            if (pool != null) {
                WitnessContext ctx = tlCtx.get();
                newPrio = estimatePriorityCtx(node, ctx);
            } else {
                newPrio = estimatePriority(node);
            }
            if (newPrio > storedPrio[node]) {
                storedPrio[node] = newPrio;
                pq.insert(node, newPrio + nodeCount);
                inPQ[node] = true;
                continue;
            }

            // Defer high-degree nodes to the final PQ phase.
            if (shouldDefer(node)) continue;

            // Contract this node — use intra-node parallelism for high-degree nodes,
            // ctx-based batched contraction for medium nodes, shared state only when sequential.
            if (pool != null && countActiveInDeg(node) >= INTRA_PAR_MIN_IN_DEGREE) {
                contractNodeIntraParallel(node, pool, tlCtx);
            } else if (pool != null) {
                WitnessContext ctx = tlCtx.get();
                contractNodeBatchedCtx(node, ctx);
            } else {
                contractNodeBatched(node);
            }
            contracted[node] = true;
            nodeLevel[node]  = ranks[contractionIdx];
            order[node]      = ranks[contractionIdx];
            contractionIdx++;

            // Re-estimate priorities of uncontracted neighbors in this cell.
            // The shortcuts just created may change their edge-difference.
            // Uses remove + insert (not decreaseKey) because priorities can
            // increase; the remove+insert pattern is safe despite DAryMinHeap.remove()
            // not resetting pos[node], because insert() always overwrites pos[node].
            reestimateCellNeighborsParallel(node, cellGen, pq, storedPrio, inPQ, pool, tlCtx);
        }
    }

    /**
     * Re-estimates cell-neighbor priorities with optional parallelism.
     *
     * <p>Collects all uncontracted cell neighbors that are still in the PQ
     * (tracked by the {@code inPQ} array), re-estimates their priorities
     * (in parallel when a pool is available and there are enough neighbors),
     * and applies updates using {@code remove + insert} to update the PQ.
     * This approach handles both increased and decreased priorities correctly.
     *
     * <p><b>Note:</b> {@code DAryMinHeap.remove()} does not reset
     * {@code pos[node]} to -1 after removal (unlike {@code poll()}).
     * The {@code remove + insert} pattern is safe because {@code insert()}
     * always starts from {@code this.size} and overwrites {@code pos[node]}
     * with the final heap position. The {@code inPQ} array provides an
     * additional guard against operating on nodes that were already polled.
     *
     * <p>Parallel re-estimation is critical for late ND depths where separator
     * cells contain hundreds of high-degree hub nodes: each re-estimation
     * involves a witness search that can settle thousands of nodes, and there
     * may be 10-50 neighbors to re-estimate after each contraction.
     */
    private void reestimateCellNeighborsParallel(int node, int cellGen, DAryMinHeap pq,
                                                  int[] storedPrio,
                                                  boolean[] inPQ,
                                                  ForkJoinPool pool,
                                                  ThreadLocal<WitnessContext> tlCtx) {
        // Collect neighbors in this cell that are still in the PQ.
        // A node can appear as both an out-neighbor and an in-neighbor;
        // use a generation-stamped guard to deduplicate.
        int nbrCount = 0;
        int[] nbrs = new int[64];
        int gen = this.nbrRemovedGeneration.incrementAndGet();

        int[] oArr = outEdges[node];
        int oLen = this.outLen[node];
        for (int i = 0; i < oLen; i++) {
            int w = buildEdgeData[oArr[i] * BE_SIZE + BE_TO];
            if (!contracted[w] && cellMemberGen[w] == cellGen
                    && inPQ[w] && nbrRemovedGen[w] != gen) {
                nbrRemovedGen[w] = gen;
                if (nbrCount >= nbrs.length) nbrs = Arrays.copyOf(nbrs, nbrs.length * 2);
                nbrs[nbrCount++] = w;
            }
        }

        int[] iArr = inEdges[node];
        int iLen = this.inLen[node];
        for (int j = 0; j < iLen; j++) {
            int u = buildEdgeData[iArr[j] * BE_SIZE + BE_FROM];
            if (!contracted[u] && cellMemberGen[u] == cellGen
                    && inPQ[u] && nbrRemovedGen[u] != gen) {
                nbrRemovedGen[u] = gen;
                if (nbrCount >= nbrs.length) nbrs = Arrays.copyOf(nbrs, nbrs.length * 2);
                nbrs[nbrCount++] = u;
            }
        }

        if (nbrCount == 0) return;

        if (pool != null && nbrCount >= 4) {
            // Parallel re-estimation: each neighbor's estimatePriorityCtx is
            // independent (reads graph state, no writes), so they can run concurrently.
            int[] newPrios = new int[nbrCount];
            @SuppressWarnings("unchecked")
            ForkJoinTask<?>[] tasks = new ForkJoinTask[nbrCount];
            for (int i = 0; i < nbrCount; i++) {
                final int idx = i;
                final int nbr = nbrs[idx];
                tasks[i] = pool.submit(() -> {
                    WitnessContext ctx = tlCtx.get();
                    newPrios[idx] = estimatePriorityCtx(nbr, ctx);
                });
            }
            for (int i = 0; i < nbrCount; i++) tasks[i].join();
            for (int i = 0; i < nbrCount; i++) {
                int w = nbrs[i];
                int newPrio = newPrios[i];
                storedPrio[w] = newPrio;
                pq.remove(w);
                pq.insert(w, newPrio + nodeCount);
            }
        } else {
            // Sequential fallback
            for (int i = 0; i < nbrCount; i++) {
                int nbr = nbrs[i];
                int newPrio = estimatePriority(nbr);
                storedPrio[nbr] = newPrio;
                pq.remove(nbr);
                pq.insert(nbr, newPrio + nodeCount);
            }
        }
    }

    /** Counts active (uncontracted, non-self) in-neighbours of a node. */
    private int countActiveInDeg(int node) {
        int[] iArr = inEdges[node];
        int iLen = this.inLen[node];
        int deg = 0;
        for (int j = 0; j < iLen; j++) {
            int u = buildEdgeData[iArr[j] * BE_SIZE + BE_FROM];
            if (!contracted[u] && u != node) deg++;
        }
        return deg;
    }

    /**
     * Minimum active in-degree for a node to use intra-node parallelism.
     * Below this threshold, the overhead of task submission exceeds the
     * benefit of parallel witness searches.
     */
    private static final int INTRA_PAR_MIN_IN_DEGREE = 6;

    /**
     * Lower threshold for the deferred phase: even with in-degree 3-4,
     * deferred hubs have very large out-degree (expensive witness searches)
     * so parallelism pays off at a lower threshold.
     */
    private static final int DEFERRED_INTRA_PAR_MIN_IN_DEGREE = 3;

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
            // Defer high-degree nodes to the final PQ phase.
            if (shouldDefer(node)) continue;

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
                // Must use thread-local Ctx variant for thread-safety when
                // this method runs in parallel across multiple cells.
                WitnessContext ctx = tlCtx.get();
                contractNodeBatchedCtx(node, ctx);
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
     *
     * <p>Tasks are submitted in bounded batches to prevent memory exhaustion
     * on high-degree separator nodes (e.g. inDeg=5000) where submitting all
     * tasks at once would allocate gigabytes of temporary arrays concurrently.
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

        // Limit concurrent tasks to prevent OOM on high-degree separator nodes.
        // Each task allocates 6 arrays of size numTargets (5 int[] + 1 double[]),
        // totalling ~(5*4 + 8)*numTargets = 28*numTargets bytes per task.
        // With thousands of in-neighbours, submitting all at once would allocate
        // gigabytes of temporary arrays.  Use 2× parallelism to keep all threads
        // busy (one task executing + one queued per thread), with a floor of 8
        // to avoid under-utilization on low-core machines.
        int maxConcurrent = Math.max(pool.getParallelism() * 2, 8);

        @SuppressWarnings("unchecked")
        ForkJoinTask<InNbrShortcuts>[] batch = new ForkJoinTask[Math.min(ni, maxConcurrent)];
        int submitted = 0;
        int nextToJoin = 0;

        for (int idx = 0; idx < ni; idx++) {
            final int u = inNbrs[idx];
            final int inIdx = inIdxArr[idx];

            batch[submitted % batch.length] = pool.submit(() -> {
                WitnessContext ctx = tlCtx.get();
                double wUV = buildEdgeWeights[inIdx];

                // Compute max cost bound
                double globalMaxCost = 0;
                for (int t = 0; t < nt; t++) {
                    if (targets[t] == u) continue;
                    double bound = wUV + tgtCosts[t];
                    if (bound > globalMaxCost) globalMaxCost = bound;
                }

                // Run witness search with early termination
                batchedWitnessSearchCtx(u, node, globalMaxCost, HOP_LIMIT,
                        effectiveSettledLimit(nt), ctx, targets, nt);

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
            submitted++;

            // When the batch is full, drain completed tasks before submitting more
            if (submitted - nextToJoin >= batch.length) {
                InNbrShortcuts sc = batch[nextToJoin % batch.length].join();
                for (int s = 0; s < sc.count; s++) {
                    addBuildEdge(sc.from[s], sc.to[s], -1,
                            sc.mid[s], sc.low1[s], sc.low2[s], sc.cost[s]);
                }
                nextToJoin++;
            }
        }

        // Drain remaining tasks
        while (nextToJoin < submitted) {
            InNbrShortcuts sc = batch[nextToJoin % batch.length].join();
            for (int s = 0; s < sc.count; s++) {
                addBuildEdge(sc.from[s], sc.to[s], -1,
                        sc.mid[s], sc.low1[s], sc.low2[s], sc.cost[s]);
            }
            nextToJoin++;
        }
    }

    /**
     * Priority estimation using a quick, limited witness search.
     * Uses reduced hop/settled limits compared to the full contraction witness search.
     * This gives a much tighter shortcut count estimate than the inDeg × outDeg upper bound
     * without the full cost of the contraction-time search.
     */
    private int estimatePriority(int node) {
        return estimatePriority(node, PRIO_HOP_LIMIT, PRIO_SETTLED_LIMIT);
    }

    /** Priority estimation with configurable witness-search limits.
     *  Used by the deferred PQ phase with higher limits for more accurate estimates. */
    private int estimatePriority(int node, int hopLimit, int settledLimit) {
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

            // Quick bounded Dijkstra from u with configurable limits.
            batchedWitnessSearch(u, node, globalMax, hopLimit, settledLimit);

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

    // ---- Priority-based cell reordering ----

    /**
     * Thread-safe priority estimation using a {@link WitnessContext}.
     * Semantically identical to {@link #estimatePriority(int)} but uses the
     * context's witness arrays, heap, and scratch space.
     */
    private int estimatePriorityCtx(int node, WitnessContext ctx) {
        return estimatePriorityCtx(node, ctx, PRIO_HOP_LIMIT, PRIO_SETTLED_LIMIT);
    }

    /** Thread-safe priority estimation with configurable witness-search limits. */
    private int estimatePriorityCtx(int node, WitnessContext ctx, int hopLimit, int settledLimit) {
        int[] oArr = outEdges[node];
        int oLen = outLen[node];
        int[] iArr = inEdges[node];
        int iLen = inLen[node];

        int numTargets = 0;
        for (int i = 0; i < oLen; i++) {
            int outIdx = oArr[i];
            int w = buildEdgeData[outIdx * BE_SIZE + BE_TO];
            if (contracted[w] || w == node) continue;
            if (numTargets >= ctx.scratchTargets.length) ctx.growScratch();
            ctx.scratchTargets[numTargets]  = w;
            ctx.scratchMaxCosts[numTargets] = buildEdgeWeights[outIdx];
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
            double globalMax = 0;
            for (int t = 0; t < numTargets; t++) {
                if (ctx.scratchTargets[t] == u) continue;
                double bound = wUV + ctx.scratchMaxCosts[t];
                if (bound > globalMax) globalMax = bound;
            }

            batchedWitnessSearchCtx(u, node, globalMax, hopLimit, settledLimit, ctx);

            for (int t = 0; t < numTargets; t++) {
                int w = ctx.scratchTargets[t];
                if (w == u) continue;
                double scCost = wUV + ctx.scratchMaxCosts[t];
                double witCost = (ctx.witnessIterIds[w] == ctx.witnessIteration)
                        ? ctx.witnessCost[w] : Double.POSITIVE_INFINITY;
                if (witCost <= scCost) continue;
                shortcuts++;
            }
        }

        return shortcuts - (inDeg + outDeg) + contractedNeighborCount[node];
    }

    /**
     * Sorts cell nodes in-place by ascending estimated priority (edge-difference
     * heuristic).  Uses the builder's shared witness state – must be called
     * from a single-threaded context.
     *
     * <p>This ensures low-degree nodes are contracted first.  Their shortcuts
     * then serve as witnesses during high-degree contractions, preventing the
     * cascading edge explosion observed on large PT+road networks.
     */
    private void sortCellByPriority(int[] cellNodes) {
        int n = cellNodes.length;
        if (n < CELL_REORDER_THRESHOLD) return;

        long[] pairs = new long[n];
        for (int i = 0; i < n; i++) {
            int prio = estimatePriority(cellNodes[i]);
            pairs[i] = ((long) prio << 32) | (cellNodes[i] & 0xFFFFFFFFL);
        }
        Arrays.sort(pairs);
        for (int i = 0; i < n; i++) {
            cellNodes[i] = (int) pairs[i];
        }
    }

    /**
     * Like {@link #sortCellByPriority(int[])} but uses a thread-local
     * {@link WitnessContext} for the priority estimation, making it safe
     * for use in parallel cell contraction.
     */
    private void sortCellByPriorityCtx(int[] cellNodes, WitnessContext ctx) {
        int n = cellNodes.length;
        if (n < CELL_REORDER_THRESHOLD) return;

        long[] pairs = new long[n];
        for (int i = 0; i < n; i++) {
            int prio = estimatePriorityCtx(cellNodes[i], ctx);
            pairs[i] = ((long) prio << 32) | (cellNodes[i] & 0xFFFFFFFFL);
        }
        Arrays.sort(pairs);
        for (int i = 0; i < n; i++) {
            cellNodes[i] = (int) pairs[i];
        }
    }

    /**
     * Contracts a node using batched witness search: for each in-neighbour u,
     * runs ONE Dijkstra from u (avoiding the contracted node) to find witnesses
     * for ALL out-neighbours w simultaneously.
     */
    private void contractNodeBatched(int node) {
        int[] oArr = outEdges[node];
        int oLen = Math.min(outLen[node], oArr.length);  // clamp: unsynchronized read
        int[] iArr = inEdges[node];
        int iLen = Math.min(inLen[node], iArr.length);   // clamp: unsynchronized read

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
            // Pass targets for early termination when all witnesses are found.
            batchedWitnessSearch(u, node, globalMaxCost, HOP_LIMIT,
                    effectiveSettledLimit(numTargets), scratchTargets, numTargets);

            // Build O(1) dedup lookup: best existing out-edge cost from u to each neighbor.
            dedupGeneration++;
            int[] uOutArr = outEdges[u];
            int uOLen = Math.min(outLen[u], uOutArr.length);  // clamp: unsynchronized read
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
     * Computes the effective settled limit for a witness search based on the
     * number of active targets.  On small-to-medium networks the base
     * {@link #SETTLED_LIMIT} is sufficient, but on large networks with high-degree
     * separator nodes the witness search must explore more nodes to discover
     * existing paths and avoid creating unnecessary shortcuts.
     *
     * <p>Uses sub-linear (sqrt) scaling instead of linear scaling.  For high-degree
     * deferred hub nodes (e.g. numTargets=4000), linear scaling of 50×numTargets
     * would settle up to 200k nodes per witness search, which dominates the
     * deferred-phase runtime.  Sub-linear scaling (500×√numTargets) provides
     * ample exploration budget while drastically reducing work for high-degree nodes:
     * <ul>
     *   <li>numTargets=10  → 6,581  (was 5,500)</li>
     *   <li>numTargets=100 → 10,000 (was 10,000)</li>
     *   <li>numTargets=1000 → 20,811 (was 55,000)</li>
     *   <li>numTargets=4000 → 36,623 (was 200,000)</li>
     * </ul>
     *
     * <p>During the deferred phase ({@link #deferredPhaseActive}), a more
     * aggressive formula with factor 200 and lower cap is used.  Deferred
     * nodes have very few active neighbours, so the effective graph is tiny
     * and a lower settled limit suffices.  Missing a witness only adds a
     * conservative shortcut (correctness is not affected).
     *
     * @param numTargets number of active (uncontracted) out-neighbours of the
     *                   node being contracted.
     * @return settled limit in {@code [SETTLED_LIMIT, MAX_SETTLED_LIMIT]}.
     */
    private int effectiveSettledLimit(int numTargets) {
        if (deferredPhaseActive) {
            // Deferred phase: aggressive scaling with lower cap.
            return Math.min(DEFERRED_MAX_SETTLED_LIMIT,
                    SETTLED_LIMIT + (int) (Math.sqrt(numTargets) * 200));
        }
        // Sub-linear scaling: sqrt(numTargets) * 500, capped at 100k.
        return Math.min(MAX_SETTLED_LIMIT,
                SETTLED_LIMIT + (int) (Math.sqrt(numTargets) * 500));
    }

    /**
     * Runs a single bounded Dijkstra from {@code source}, avoiding {@code avoidNode},
     * up to {@code maxCost} and {@code hopLimit} hops. After return, callers can
     * read {@code witnessCost[target]} for any target to check if a witness exists.
     *
     * <p><b>Early termination</b>: if {@code targets} and {@code numTargets} are
     * provided, the search stops as soon as all targets have been settled, avoiding
     * unnecessary exploration that dominates runtime for high-degree deferred hubs.
     */
    private void batchedWitnessSearch(int source, int avoidNode, double maxCost,
                                      int hopLimit, int settledLimit) {
        batchedWitnessSearch(source, avoidNode, maxCost, hopLimit, settledLimit, null, 0);
    }

    private void batchedWitnessSearch(int source, int avoidNode, double maxCost,
                                      int hopLimit, int settledLimit,
                                      int[] targets, int numTargets) {
        witnessIteration++;
        witnessHeap.clear();

        // Mark targets for O(1) membership check (generation-stamped)
        int tgtGen = 0;
        if (targets != null && numTargets > 0) {
            tgtGen = ++targetGeneration;
            for (int t = 0; t < numTargets; t++) targetGen[targets[t]] = tgtGen;
        }

        witnessIterIds[source] = witnessIteration;
        witnessCost[source]    = 0.0;
        witnessHops[source]    = 0;
        witnessHeap.insert(source, 0.0);

        int settled = 0;
        int targetsFound = 0;
        while (!witnessHeap.isEmpty()) {
            int    v    = witnessHeap.poll();
            double cost = witnessCost[v];
            int    hops = witnessHops[v];

            if (cost > maxCost)   break; // everything remaining exceeds bound
            if (hops >= hopLimit) continue;
            if (++settled > settledLimit) break; // prevent unbounded exploration

            // Early termination: O(1) check if v is a target
            if (tgtGen != 0 && targetGen[v] == tgtGen) {
                if (++targetsFound >= numTargets) break;
            }

            int[] vOutArr = outEdges[v];
            int vOLen = Math.min(outLen[v], vOutArr.length);  // clamp: unsynchronized read
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

            batchedWitnessSearchCtx(u, node, globalMaxCost, HOP_LIMIT,
                    effectiveSettledLimit(numTargets), ctx, ctx.scratchTargets, numTargets);

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
     *
     * <p>Supports early termination via optional target array.
     */
    private void batchedWitnessSearchCtx(int source, int avoidNode, double maxCost,
                                          int hopLimit, int settledLimit,
                                          WitnessContext ctx) {
        batchedWitnessSearchCtx(source, avoidNode, maxCost, hopLimit, settledLimit, ctx, null, 0);
    }

    private void batchedWitnessSearchCtx(int source, int avoidNode, double maxCost,
                                          int hopLimit, int settledLimit,
                                          WitnessContext ctx,
                                          int[] targets, int numTargets) {
        ctx.witnessIteration++;
        ctx.witnessHeap.clear();

        // Mark targets for O(1) membership check (generation-stamped, thread-local)
        int tgtGen = 0;
        if (targets != null && numTargets > 0) {
            tgtGen = ++ctx.targetGeneration;
            for (int t = 0; t < numTargets; t++) ctx.targetGen[targets[t]] = tgtGen;
        }

        ctx.witnessIterIds[source] = ctx.witnessIteration;
        ctx.witnessCost[source]    = 0.0;
        ctx.witnessHops[source]    = 0;
        ctx.witnessHeap.insert(source, 0.0);

        int settled = 0;
        int targetsFound = 0;
        while (!ctx.witnessHeap.isEmpty()) {
            int    v    = ctx.witnessHeap.poll();
            double cost = ctx.witnessCost[v];
            int    hops = ctx.witnessHops[v];

            if (cost > maxCost)   break;
            if (hops >= hopLimit) continue;
            if (++settled > settledLimit) break;

            // Early termination: O(1) check if v is a target
            if (tgtGen != 0 && ctx.targetGen[v] == tgtGen) {
                if (++targetsFound >= numTargets) break;
            }

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
    // Adjacency compaction — removes stale references to contracted nodes
    // -------------------------------------------------------------------------

    /**
     * Compacts the adjacency lists of all <b>uncontracted</b> nodes by removing
     * entries that point to already-contracted nodes.  Also compacts the
     * adjacency lists of the <em>active neighbours</em> of uncontracted nodes
     * (the 1-hop frontier), because witness searches explore those lists too.
     *
     * <p>After the ND rounds, ~96% of nodes are contracted, so the adjacency
     * lists of the remaining 4% are bloated with stale entries.  Every witness
     * search iterates over these entries only to skip them with
     * {@code if (contracted[toNode]) continue}.  Compacting removes this
     * overhead entirely.
     *
     * <p>This is safe because contracted nodes are never "un-contracted":
     * once {@code contracted[n] == true}, no code ever reads the adjacency
     * lists of n for contraction purposes (only the build-edge data matters
     * for the final CSR build, and that is indexed by build-edge counter, not
     * by adjacency lists).
     *
     * @param nodes the set of uncontracted node indices to compact.
     *              Their active neighbours are also compacted.
     */
    private void compactAdjacencyLists(int[] nodes) {
        // Phase 1: Mark nodes whose adjacency lists need compaction.
        // Use a generation stamp to avoid duplicate processing.
        int gen = this.nbrRemovedGeneration.incrementAndGet();

        // First, mark the uncontracted nodes themselves.
        for (int node : nodes) {
            nbrRemovedGen[node] = gen;
        }

        // Also mark their active (uncontracted) neighbours.
        for (int node : nodes) {
            int[] oArr = outEdges[node];
            int oLen = outLen[node];
            for (int i = 0; i < oLen; i++) {
                int w = buildEdgeData[oArr[i] * BE_SIZE + BE_TO];
                if (!contracted[w]) nbrRemovedGen[w] = gen;
            }
            int[] iArr = inEdges[node];
            int iLen = inLen[node];
            for (int j = 0; j < iLen; j++) {
                int u = buildEdgeData[iArr[j] * BE_SIZE + BE_FROM];
                if (!contracted[u]) nbrRemovedGen[u] = gen;
            }
        }

        // Phase 2: Compact adjacency lists of all marked nodes.
        for (int n = 0; n < nodeCount; n++) {
            if (nbrRemovedGen[n] != gen) continue;
            compactOutEdges(n);
            compactInEdges(n);
        }
    }

    /**
     * Lightweight compaction: compact only the given nodes (no neighbour walk).
     * Used for periodic re-compaction during the deferred phase where the
     * frontier is implicit (all remaining uncontracted nodes).
     */
    private void compactAdjacencyListsDirect(int[] nodes, int count) {
        for (int i = 0; i < count; i++) {
            int n = nodes[i];
            if (contracted[n]) continue;
            compactOutEdges(n);
            compactInEdges(n);
        }
    }

    private void compactOutEdges(int node) {
        synchronized (adjLocks[node & (ADJ_LOCK_STRIPES - 1)]) {
            int[] arr = outEdges[node];
            int len = outLen[node];
            int write = 0;
            for (int read = 0; read < len; read++) {
                int edgeIdx = arr[read];
                int toNode = buildEdgeData[edgeIdx * BE_SIZE + BE_TO];
                if (!contracted[toNode]) {
                    arr[write++] = edgeIdx;
                }
            }
            outLen[node] = write;
        }
    }

    private void compactInEdges(int node) {
        synchronized (adjLocks[node & (ADJ_LOCK_STRIPES - 1)]) {
            int[] arr = inEdges[node];
            int len = inLen[node];
            int write = 0;
            for (int read = 0; read < len; read++) {
                int edgeIdx = arr[read];
                int fromNode = buildEdgeData[edgeIdx * BE_SIZE + BE_FROM];
                if (!contracted[fromNode]) {
                    arr[write++] = edgeIdx;
                }
            }
            inLen[node] = write;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3 – build CHGraph (CSR layout)
    // -------------------------------------------------------------------------

    private CHGraph buildCHGraph() {
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
        int S = CHGraph.E_STRIDE;
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

        // 4. Sort build edges deterministically before filling CSR arrays.
        // Parallel contraction may add shortcuts in non-deterministic order
        // (different AtomicInteger increments per run).  Sorting each node's
        // edges by (neighborNode, origLink) ensures the CSR is always identical.

        // Collect up-edge build indices per source node, dn-edge per target node.
        int[][] upBiPerNode = new int[nodeCount][];
        int[][] dnBiPerNode = new int[nodeCount][];
        { // scope for temporary arrays
            int[] upCntTmp = new int[nodeCount];
            int[] dnCntTmp = new int[nodeCount];
            for (int bi = 0; bi < buildEdgeCount; bi++) {
                int bBase = bi * BE_SIZE;
                int fromNode = buildEdgeData[bBase + BE_FROM];
                int toNode   = buildEdgeData[bBase + BE_TO];
                int lvFrom = nodeLevel[fromNode];
                int lvTo   = nodeLevel[toNode];
                if (lvFrom < lvTo) upCntTmp[fromNode]++;
                else if (lvFrom > lvTo) dnCntTmp[toNode]++;
            }
            for (int n = 0; n < nodeCount; n++) {
                if (upCntTmp[n] > 0) upBiPerNode[n] = new int[upCntTmp[n]];
                if (dnCntTmp[n] > 0) dnBiPerNode[n] = new int[dnCntTmp[n]];
                upCntTmp[n] = 0;
                dnCntTmp[n] = 0;
            }
            for (int bi = 0; bi < buildEdgeCount; bi++) {
                int bBase = bi * BE_SIZE;
                int fromNode = buildEdgeData[bBase + BE_FROM];
                int toNode   = buildEdgeData[bBase + BE_TO];
                int lvFrom = nodeLevel[fromNode];
                int lvTo   = nodeLevel[toNode];
                if (lvFrom < lvTo) upBiPerNode[fromNode][upCntTmp[fromNode]++] = bi;
                else if (lvFrom > lvTo) dnBiPerNode[toNode][dnCntTmp[toNode]++] = bi;
            }
        }

        // Sort each node's edges by (neighborNode, origLink) for determinism.
        for (int n = 0; n < nodeCount; n++) {
            if (upBiPerNode[n] != null && upBiPerNode[n].length > 1)
                sortBuildEdgesBy(upBiPerNode[n], buildEdgeData, BE_SIZE, BE_TO, BE_ORIG);
            if (dnBiPerNode[n] != null && dnBiPerNode[n].length > 1)
                sortBuildEdgesBy(dnBiPerNode[n], buildEdgeData, BE_SIZE, BE_FROM, BE_ORIG);
        }

        // Fill CSR arrays in deterministic sorted order.
        for (int n = 0; n < nodeCount; n++) {
            if (upBiPerNode[n] != null) {
                int off = upOff[n];
                for (int k = 0; k < upBiPerNode[n].length; k++) {
                    int bi    = upBiPerNode[n][k];
                    int bBase = bi * BE_SIZE;
                    int slot  = off + k;
                    int eBase = slot * S;
                    upEdges[eBase + CHGraph.E_NODE] = buildEdgeData[bBase + BE_TO];
                    upEdges[eBase + CHGraph.E_GIDX] = slot;
                    upWeights[slot] = buildEdgeWeights[bi];
                    edgeOrigLink[slot] = buildEdgeData[bBase + BE_ORIG];
                    edgeLower1[slot] = buildEdgeData[bBase + BE_LOW1];
                    edgeLower2[slot] = buildEdgeData[bBase + BE_LOW2];
                    oldToNew[bi] = slot;
                }
            }
            if (dnBiPerNode[n] != null) {
                int off = dnOff[n];
                for (int k = 0; k < dnBiPerNode[n].length; k++) {
                    int bi     = dnBiPerNode[n][k];
                    int bBase  = bi * BE_SIZE;
                    int dnSlot = off + k;
                    int gIdx   = totalUp + dnSlot;
                    int eBase  = dnSlot * S;
                    dnEdges[eBase + CHGraph.E_NODE] = buildEdgeData[bBase + BE_FROM];
                    dnEdges[eBase + CHGraph.E_GIDX] = gIdx;
                    dnWeights[dnSlot] = buildEdgeWeights[bi];
                    edgeOrigLink[gIdx] = buildEdgeData[bBase + BE_ORIG];
                    edgeLower1[gIdx] = buildEdgeData[bBase + BE_LOW1];
                    edgeLower2[gIdx] = buildEdgeData[bBase + BE_LOW2];
                    oldToNew[bi] = gIdx;
                }
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
        // Process build edges in node-level order (lower levels first) to ensure
        // dependencies (lower edges created before parent shortcuts) are preserved.
        // Within the same level, edges are in deterministic sorted order.
        int[] customizeOrder = new int[totalEdgeCount];
        int orderIdx = 0;
        for (int bi = 0; bi < buildEdgeCount; bi++) {
            if (oldToNew[bi] >= 0) {
                customizeOrder[orderIdx++] = oldToNew[bi];
            }
        }

        return new CHGraph(graph, nodeCount,
                totalUp, upOff, upCount, upEdges, upWeights,
                totalDn, dnOff, dnCount, dnEdges, dnWeights,
                totalEdgeCount, edgeOrigLink, edgeLower1, edgeLower2,
                customizeOrder, nodeLevel);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Sorts an array of build-edge indices by (primaryField, secondaryField) of
     * the build-edge data.  This ensures deterministic CSR filling order
     * regardless of parallel contraction thread scheduling.
     */
    private static void sortBuildEdgesBy(int[] biArray, int[] buildEdgeData,
                                          int beSize, int primaryField, int secondaryField) {
        // Pack (primary, secondary, buildIndex) into a long for one-pass sort.
        long[] keys = new long[biArray.length];
        for (int i = 0; i < biArray.length; i++) {
            int bi = biArray[i];
            int bBase = bi * beSize;
            int primary   = buildEdgeData[bBase + primaryField];
            int secondary = buildEdgeData[bBase + secondaryField];
            // primary in upper 32 bits, secondary as unsigned in lower 32
            keys[i] = ((long) primary << 32) | (secondary & 0xFFFFFFFFL);
        }
        // Co-sort keys and biArray
        // Simple insertion sort is fine for typical edge counts per node (< 100)
        for (int i = 1; i < keys.length; i++) {
            long kv = keys[i];
            int bv = biArray[i];
            int j = i - 1;
            while (j >= 0 && keys[j] > kv) {
                keys[j + 1] = keys[j];
                biArray[j + 1] = biArray[j];
                j--;
            }
            keys[j + 1] = kv;
            biArray[j + 1] = bv;
        }
    }

    private int addBuildEdge(int from, int to, int origLink,
                             int middle, int lower1, int lower2, double weight) {
        int idx = buildEdgeCounter.getAndIncrement();
        int base = idx * BE_SIZE;
        int minDataLen = base + BE_SIZE;
        int minWeightLen = idx + 1;
        // Grow if needed.
        if (minDataLen > buildEdgeData.length) {
            growBuildEdgeStorage(minDataLen);
        }
        // Re-read volatile references after potential grow.  Both arrays are
        // updated inside the synchronized growBuildEdgeStorage, but another
        // thread may be in the middle of growing — spin-wait until BOTH
        // arrays are large enough.  This handles the non-atomic update of
        // the two volatile fields.
        int[] edgeData = this.buildEdgeData;
        double[] edgeWeights = this.buildEdgeWeights;
        while (minDataLen > edgeData.length || minWeightLen > edgeWeights.length) {
            Thread.onSpinWait();
            edgeData = this.buildEdgeData;
            edgeWeights = this.buildEdgeWeights;
        }
        edgeData[base + BE_FROM] = from;
        edgeData[base + BE_TO]   = to;
        edgeData[base + BE_ORIG] = origLink;
        edgeData[base + BE_MID]  = middle;
        edgeData[base + BE_LOW1] = lower1;
        edgeData[base + BE_LOW2] = lower2;
        edgeWeights[idx] = weight;
        adjOutAdd(from, idx);
        adjInAdd(to, idx);
        return idx;
    }

    /** Grow buildEdgeData/buildEdgeWeights under a lock (rare path).
     *  Uses 1.5× growth to reduce peak memory vs 2× doubling.
     *  Extra headroom (+1024) prevents repeated grow calls when multiple
     *  threads trigger growth nearly simultaneously.
     *  IMPORTANT: buildEdgeWeights is written BEFORE buildEdgeData so that
     *  threads spinning on both lengths see a consistent pair (the weights
     *  array is never smaller than what the data array implies). */
    private synchronized void growBuildEdgeStorage(int minDataLen) {
        if (minDataLen <= buildEdgeData.length) return; // another thread grew it
        int newLen = Math.max(buildEdgeData.length + buildEdgeData.length / 2,
                              minDataLen + 1024 * BE_SIZE);
        // Copy weights first, then data — ensures any thread that sees the new
        // buildEdgeData also sees a buildEdgeWeights that is at least as large.
        double[] newWeights = Arrays.copyOf(buildEdgeWeights, newLen / BE_SIZE);
        int[] newData = Arrays.copyOf(buildEdgeData, newLen);
        buildEdgeWeights = newWeights;  // volatile write #1
        buildEdgeData    = newData;     // volatile write #2
    }

}
