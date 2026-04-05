package org.matsim.core.router.speedy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes a nested-dissection node ordering for a {@link SpeedyGraph} using
 * coordinate-based inertial flow partitioning with graph-based refinement.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Project all nodes in the sub-graph onto several axis directions
 *       (horizontal, vertical, and two diagonals).</li>
 *   <li>For each direction, split nodes at the projection median into two halves.
 *       The direction with the best (smallest, most balanced) separator is used.</li>
 *   <li>The boundary is converted to a <b>one-sided vertex separator</b>: only
 *       nodes on the smaller boundary side become separator nodes.</li>
 *   <li><b>Degree-weighted scoring</b>: separators are scored by
 *       &sum;(subgraphDeg&sup2;) rather than uniform node count, since each
 *       separator node creates O(deg&sup2;) shortcuts during CH contraction.</li>
 *   <li><b>FM refinement</b>: Fiduccia&ndash;Mattheyses local search minimises
 *       the edge cut of the underlying bipartition, using a bucket linked-list
 *       for O(1) max-gain lookup.  Up to 5 passes until fixpoint.</li>
 *   <li><b>Max-flow refinement</b>: Dinic's algorithm on a node-split flow
 *       network finds the minimum vertex separator between the two partition
 *       sides.  Each internal node is split into v_in/v_out with capacity 1;
 *       original edges have capacity &infin;.</li>
 *   <li>The separator nodes receive the highest contraction levels.
 *       The two partitions are recursively dissected.</li>
 * </ol>
 *
 * <p>This eliminates witness searches during ordering and is dramatically faster
 * than priority-queue-based ordering for large networks.
 *
 * <p>References: Dibbelt et al. (2016) "Customizable Contraction Hierarchies",
 * Hamann &amp; Strasser (2018) "Graph Bisection with Pareto Optimization",
 * Fiduccia &amp; Mattheyses (1982) "A Linear-Time Heuristic for Improving
 * Network Partitions".
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class InertialFlowCutter {

    /**
     * Result of {@link #computeOrderWithBatches()}: contraction order plus
     * partition structure for parallel contraction.
     *
     * <p>{@code rounds} is a list of "rounds" processed sequentially.
     * Each round contains a list of independent cells that can be contracted
     * in parallel.  Within each cell, nodes are in contraction order and must
     * be processed sequentially.
     *
     * <p>Rounds are ordered from the deepest ND level (leaf cells, contracted
     * first) to the shallowest (root separator, contracted last).
     */
    public static class NDOrderResult {
        public final int[] order;
        public final List<List<int[]>> rounds;

        NDOrderResult(int[] order, List<List<int[]>> rounds) {
            this.order = order;
            this.rounds = rounds;
        }
    }

    private static final Logger LOG = LogManager.getLogger(InertialFlowCutter.class);

    /** Minimum sub-graph size below which we stop recursing and order arbitrarily. */
    private static final int MIN_PARTITION_SIZE = 2;

    /** Minimum subgraph size to apply FM refinement. */
    private static final int FM_MIN_SIZE = 20;

    /** Maximum FM passes per separator refinement. */
    private static final int FM_MAX_PASSES = 5;

    /** Minimum subgraph size to apply max-flow refinement. */
    private static final int MAXFLOW_MIN_SIZE = 30;

    /** BFS depth from boundary for max-flow cuttable region. */
    private static final int MAXFLOW_BORDER_DEPTH = 8;

    private final SpeedyGraph graph;

    // Node coordinates (extracted once)
    private final double[] nodeX;
    private final double[] nodeY;

    // Reusable scratch arrays (sized to nodeCount)
    private final int[] scratchSide;
    private final int[] scratchBoundary;  // generation-stamped boundary markers
    private int scratchBoundaryGen = 0;

    // Shared subgraph membership (set/cleared within findSeparator)
    private final boolean[] inSubGraph;

    // Pre-allocated scratch arrays for FM refinement
    private final int[] fmGain;
    private final int[] fmNext;
    private final int[] fmPrev;

    public InertialFlowCutter(SpeedyGraph graph) {
        this.graph = graph;
        int n = graph.nodeCount;
        this.nodeX = new double[n];
        this.nodeY = new double[n];

        for (int i = 0; i < n; i++) {
            var node = graph.getNode(i);
            if (node != null) {
                nodeX[i] = node.getCoord().getX();
                nodeY[i] = node.getCoord().getY();
            }
        }

        this.scratchSide = new int[n];
        this.scratchBoundary = new int[n];
        this.inSubGraph = new boolean[n];
        this.fmGain = new int[n];
        this.fmNext = new int[n];
        this.fmPrev = new int[n];
    }

    /**
     * Computes a nested-dissection contraction order.
     * @return array where {@code order[i]} is the contraction level of node {@code i}
     *         (0 = contracted first, nodeCount-1 = contracted last).
     */
    public int[] computeOrder() {
        int n = graph.nodeCount;
        int[] order = new int[n];
        Arrays.fill(order, -1);

        // Collect all valid node indices
        int[] nodes = new int[n];
        int count = 0;
        for (int i = 0; i < n; i++) {
            if (graph.getNode(i) != null) {
                nodes[count++] = i;
            }
        }
        nodes = Arrays.copyOf(nodes, count);

        // Build symmetric adjacency for the base graph (undirected view)
        int[][] adj = buildSymmetricAdjacency();

        int[] levelCounter = new int[]{0};
        recursiveDissect(nodes, adj, order, levelCounter);

        // Fill any unassigned (isolated / null nodes)
        for (int i = 0; i < n; i++) {
            if (order[i] < 0) {
                order[i] = levelCounter[0]++;
            }
        }

        LOG.info("Nested dissection ordering computed for {} nodes", n);
        return order;
    }

    /**
     * Computes a nested-dissection contraction order <b>plus</b> a partition
     * structure for parallel contraction.
     *
     * <p>Nodes in different cells within the same round are guaranteed to be
     * independent (no edges between them), so they can be contracted in
     * parallel.  Within each cell, nodes must be contracted sequentially.
     *
     * @return {@link NDOrderResult} containing the contraction order and
     *         a list of rounds of independent cells.
     */
    public NDOrderResult computeOrderWithBatches() {
        int n = graph.nodeCount;
        int[] order = new int[n];
        Arrays.fill(order, -1);

        int[] nodes = new int[n];
        int count = 0;
        for (int i = 0; i < n; i++) {
            if (graph.getNode(i) != null) {
                nodes[count++] = i;
            }
        }
        nodes = Arrays.copyOf(nodes, count);

        int[][] adj = buildSymmetricAdjacency();

        Map<Integer, List<int[]>> cellsByDepth = new HashMap<>();
        int[] levelCounter = new int[]{0};
        recursiveDissectWithBatches(nodes, adj, order, levelCounter, cellsByDepth, 0);

        // Fill any unassigned nodes
        for (int i = 0; i < n; i++) {
            if (order[i] < 0) {
                order[i] = levelCounter[0]++;
            }
        }

        // Convert to rounds: highest depth first (leaf cells contracted first)
        int maxDepth = 0;
        for (int d : cellsByDepth.keySet()) {
            if (d > maxDepth) maxDepth = d;
        }
        List<List<int[]>> rounds = new ArrayList<>();
        for (int d = maxDepth; d >= 0; d--) {
            List<int[]> cells = cellsByDepth.get(d);
            if (cells != null && !cells.isEmpty()) {
                rounds.add(cells);
            }
        }

        LOG.info("Nested dissection ordering with batches: {} nodes, {} rounds", n, rounds.size());
        return new NDOrderResult(order, rounds);
    }

    /**
     * Recursive dissection that also tracks partition cells by depth.
     */
    private void recursiveDissectWithBatches(int[] subNodes, int[][] adj, int[] order,
                                              int[] levelCounter,
                                              Map<Integer, List<int[]>> cellsByDepth,
                                              int depth) {
        if (subNodes.length <= MIN_PARTITION_SIZE) {
            // Base case: this is a leaf cell
            int ci = 0;
            int[] cellOrdered = new int[subNodes.length];
            for (int node : subNodes) {
                if (order[node] < 0) {
                    order[node] = levelCounter[0]++;
                    cellOrdered[ci++] = node;
                }
            }
            if (ci > 0) {
                cellsByDepth.computeIfAbsent(depth, k -> new ArrayList<>())
                        .add(Arrays.copyOf(cellOrdered, ci));
            }
            return;
        }

        int[][] result = findSeparator(subNodes, adj);
        int[] partA     = result[0];
        int[] separator = result[1];
        int[] partB     = result[2];

        // Recurse into partitions (children go to deeper levels)
        recursiveDissectWithBatches(partA, adj, order, levelCounter, cellsByDepth, depth + 1);
        recursiveDissectWithBatches(partB, adj, order, levelCounter, cellsByDepth, depth + 1);

        // Separator nodes form a cell at this depth
        int[] sepOrdered = new int[separator.length];
        int si = 0;
        for (int node : separator) {
            if (order[node] < 0) {
                order[node] = levelCounter[0]++;
                sepOrdered[si++] = node;
            }
        }
        if (si > 0) {
            cellsByDepth.computeIfAbsent(depth, k -> new ArrayList<>())
                    .add(Arrays.copyOf(sepOrdered, si));
        }
    }

    /**
     * Recursively bisect the sub-graph and assign contraction levels.
     * Separator nodes get the highest levels; partition nodes are recursed into.
     */
    private void recursiveDissect(int[] subNodes, int[][] adj, int[] order, int[] levelCounter) {
        if (subNodes.length <= MIN_PARTITION_SIZE) {
            // Base case: assign levels in arbitrary order
            for (int node : subNodes) {
                if (order[node] < 0) {
                    order[node] = levelCounter[0]++;
                }
            }
            return;
        }

        // Find best separator via inertial flow
        int[][] result = findSeparator(subNodes, adj);
        int[] partA     = result[0];
        int[] separator = result[1];
        int[] partB     = result[2];

        // Recurse into smaller partitions first (contracted earlier = lower level)
        recursiveDissect(partA, adj, order, levelCounter);
        recursiveDissect(partB, adj, order, levelCounter);

        // Separator nodes get highest levels (contracted last)
        for (int node : separator) {
            if (order[node] < 0) {
                order[node] = levelCounter[0]++;
            }
        }
    }

    /**
     * Find a separator for the given sub-graph using inertial flow with
     * graph-based refinement (FM + max-flow).
     * Returns [partitionA, separator, partitionB].
     */
    private int[][] findSeparator(int[] subNodes, int[][] adj) {
        // Try 16 projection directions covering a range of angles.
        double[][] directions = {
            {1, 0}, {0, 1}, {1, 1}, {1, -1},        // 0°, 90°, 45°, 135°
            {2, 1}, {1, 2}, {2, -1}, {1, -2},        // ~27°, ~63°, ~153°, ~117°
            {3, 1}, {1, 3}, {3, -1}, {1, -3},        // ~18.4°, ~71.6°, ~161.6°, ~108.4°
            {4, 1}, {1, 4}, {4, -1}, {1, -4}         // ~14.0°, ~76.0°, ~166.0°, ~104.0°
        };

        // Build membership set once, shared across all directions and refinements
        for (int node : subNodes) inSubGraph[node] = true;

        int[][] bestResult = null;
        long bestScore = Long.MAX_VALUE;

        for (double[] dir : directions) {
            int[][] result = tryDirection(subNodes, adj, dir[0], dir[1]);
            if (result != null) {
                int balance = Math.abs(result[0].length - result[2].length);
                long score = scoreSimple(result[1], balance);
                if (score < bestScore) {
                    bestScore = score;
                    bestResult = result;
                }
            }
        }

        if (bestResult == null) {
            for (int node : subNodes) inSubGraph[node] = false;
            return trivialSplit(subNodes);
        }

        // Recompute baseline score with degree-weighted scoring for
        // fair comparison against FM/max-flow refinements
        long refScore = scoreSeparator(bestResult[1],
                Math.abs(bestResult[0].length - bestResult[2].length), adj);

        // --- FM refinement: try BOTH bipartition orientations ---
        if (subNodes.length >= FM_MIN_SIZE) {
            bestResult = tryFMBothOrientations(bestResult, adj, subNodes, refScore);
            refScore = scoreSeparator(bestResult[1],
                    Math.abs(bestResult[0].length - bestResult[2].length), adj);
        }

        // --- Max-flow refinement with wider cuttable region ---
        if (subNodes.length >= MAXFLOW_MIN_SIZE) {
            int[][] mfResult = maxFlowRefineWideDepth(bestResult, adj, MAXFLOW_BORDER_DEPTH);
            if (mfResult != null && isValidSeparator(mfResult, adj)) {
                int mfBal = Math.abs(mfResult[0].length - mfResult[2].length);
                long mfScore = scoreSeparator(mfResult[1], mfBal, adj);
                if (mfScore < refScore) {
                    bestResult = mfResult;
                    refScore = mfScore;
                }
            }
        }

        // Cleanup membership set
        for (int node : subNodes) inSubGraph[node] = false;

        return bestResult;
    }

    // ========================= Degree-weighted scoring =========================

    /**
     * Try FM refinement in both bipartition orientations, return best result.
     */
    private int[][] tryFMBothOrientations(int[][] current, int[][] adj,
                                           int[] subNodes, long currentScore) {
        int[][] best = current;
        long bestScore = currentScore;

        // Orientation 1: sideA = partA, sideB = sep ∪ partB
        int[][] fmResult1 = fmRefineSeparator(best, adj, subNodes, false);
        if (isValidSeparator(fmResult1, adj)) {
            int fmBal = Math.abs(fmResult1[0].length - fmResult1[2].length);
            long fmScore = scoreSeparator(fmResult1[1], fmBal, adj);
            if (fmScore < bestScore) {
                best = fmResult1;
                bestScore = fmScore;
            }
        }
        // Orientation 2: sideA = partA ∪ sep, sideB = partB
        int[][] fmResult2 = fmRefineSeparator(best, adj, subNodes, true);
        if (isValidSeparator(fmResult2, adj)) {
            int fmBal = Math.abs(fmResult2[0].length - fmResult2[2].length);
            long fmScore = scoreSeparator(fmResult2[1], fmBal, adj);
            if (fmScore < bestScore) {
                best = fmResult2;
            }
        }
        return best;
    }

    /**
     * Score a separator using degree-weighted cost: 256 per node (dominant) plus
     * &sum;(subgraphDeg&sup2;) as refinement.  The 256 base ensures separator SIZE
     * remains the primary criterion; the degree term breaks ties in favour of
     * low-degree separators that create fewer shortcuts.
     *
     * @param separator separator nodes
     * @param balance   partition imbalance |partA| - |partB|
     * @param adj       symmetric adjacency lists
     * @return weighted score (lower is better)
     */
    private long scoreSeparator(int[] separator, int balance, int[][] adj) {
        long cost = 0;
        for (int s : separator) {
            int subDeg = 0;
            for (int w : adj[s]) {
                if (inSubGraph[w]) subDeg++;
            }
            cost += 256L + (long) subDeg * subDeg;
        }
        return cost + balance;
    }

    /** Simple size-based scoring matching the original formula. */
    private static long scoreSimple(int[] separator, int balance) {
        return separator.length * 256L + balance;
    }

    /**
     * Check that the separator is valid: no edge connects partA directly to partB.
     * Uses the shared {@link #inSubGraph} membership set.
     */
    private boolean isValidSeparator(int[][] result, int[][] adj) {
        int[] partA = result[0];
        int[] sep   = result[1];
        int[] partB = result[2];
        if (partA.length == 0 || partB.length == 0 || sep.length == 0) return false;

        // Mark separator and partB with generation stamps
        int genSep = ++scratchBoundaryGen;
        int genB   = ++scratchBoundaryGen;
        int[] mark = scratchBoundary;
        for (int s : sep) mark[s] = genSep;
        for (int b : partB) mark[b] = genB;

        // Check: no partA node has a partB neighbour (within subgraph)
        for (int a : partA) {
            for (int w : adj[a]) {
                if (inSubGraph[w] && mark[w] == genB) {
                    return false; // direct A→B edge → invalid separator
                }
            }
        }
        return true;
    }

    // ========================= FM Refinement ==================================

    /**
     * Fiduccia–Mattheyses refinement: operate on the bipartition underlying
     * the separator to minimise the edge cut, then re-extract the separator.
     *
     * <p>Uses a bucket linked-list indexed by gain value for O(1) max-gain
     * lookup.  Each FM pass is O(n &times; avgDeg).
     *
     * @param result   current [partA, separator, partB]
     * @param adj      symmetric adjacency lists
     * @param subNodes all nodes in the sub-graph
     * @param sepToA   if true, separator joins sideA; if false, separator joins sideB
     * @return improved [partA, separator, partB], or the original if no improvement
     */
    private int[][] fmRefineSeparator(int[][] result, int[][] adj, int[] subNodes,
                                       boolean sepToA) {
        int[] partA = result[0];
        int[] sep   = result[1];
        int[] partB = result[2];

        int totalN = partA.length + sep.length + partB.length;
        if (totalN < FM_MIN_SIZE || sep.length <= 1) return result;

        // Build allNodes array (partA, sep, partB)
        int[] allNodes = new int[totalN];
        int idx = 0;
        System.arraycopy(partA, 0, allNodes, 0, partA.length);
        System.arraycopy(sep, 0, allNodes, partA.length, sep.length);
        System.arraycopy(partB, 0, allNodes, partA.length + sep.length, partB.length);

        // Setup bipartition based on orientation
        int[] side = scratchSide;
        if (sepToA) {
            // sideA = partA ∪ sep, sideB = partB
            for (int v : partA) side[v] = 1;
            for (int v : sep) side[v] = 1;
            for (int v : partB) side[v] = 2;
        } else {
            // sideA = partA, sideB = sep ∪ partB
            for (int v : partA) side[v] = 1;
            for (int v : sep) side[v] = 2;
            for (int v : partB) side[v] = 2;
        }

        int sizeA = sepToA ? (partA.length + sep.length) : partA.length;
        int sizeB = sepToA ? partB.length : (sep.length + partB.length);
        int minSize = Math.max(2, totalN / 4);

        // Compute initial gains and find max subgraph degree
        int[] gain = fmGain;
        int maxDeg = 0;
        for (int i = 0; i < totalN; i++) {
            int v = allNodes[i];
            int ext = 0, internal = 0;
            for (int w : adj[v]) {
                if (!inSubGraph[w]) continue;
                if (side[w] != side[v]) ext++;
                else internal++;
            }
            gain[v] = ext - internal;
            int deg = ext + internal;
            if (deg > maxDeg) maxDeg = deg;
        }

        if (maxDeg == 0) return result;

        // Bucket linked-list: gain g → bucket index (g + maxDeg)
        int bucketOffset = maxDeg;
        int numBuckets = 2 * maxDeg + 1;
        int[] bucketHead = new int[numBuckets];
        int[] fmN = fmNext;
        int[] fmP = fmPrev;

        // FM passes
        boolean improved = true;
        for (int pass = 0; pass < FM_MAX_PASSES && improved; pass++) {
            improved = false;

            // Recompute gains at start of each pass
            if (pass > 0) {
                maxDeg = 0;
                for (int i = 0; i < totalN; i++) {
                    int v = allNodes[i];
                    int ext = 0, internal = 0;
                    for (int w : adj[v]) {
                        if (!inSubGraph[w]) continue;
                        if (side[w] != side[v]) ext++;
                        else internal++;
                    }
                    gain[v] = ext - internal;
                    int deg = ext + internal;
                    if (deg > maxDeg) maxDeg = deg;
                }
                if (maxDeg == 0) break;
                bucketOffset = maxDeg;
                numBuckets = 2 * maxDeg + 1;
                if (bucketHead.length < numBuckets) {
                    bucketHead = new int[numBuckets];
                }
            }

            // Initialize buckets
            Arrays.fill(bucketHead, 0, numBuckets, -1);
            for (int i = 0; i < totalN; i++) {
                int v = allNodes[i];
                fmN[v] = -1;
                fmP[v] = -1;
            }

            // Insert all nodes into buckets
            for (int i = 0; i < totalN; i++) {
                int v = allNodes[i];
                int b = gain[v] + bucketOffset;
                if (b < 0) b = 0;
                if (b >= numBuckets) b = numBuckets - 1;
                fmBucketInsert(v, b, bucketHead, fmN, fmP);
            }

            // Lock via generation stamp
            int lockGen = ++scratchBoundaryGen;
            int[] locked = scratchBoundary;

            // Track moves for rollback
            int[] moveNode = new int[totalN];
            int[] moveFrom = new int[totalN];
            int bestCumGain = 0;
            int bestStep = -1;
            int cumGain = 0;
            int step = 0;
            int topBucket = numBuckets - 1;  // highest non-empty bucket hint

            while (step < totalN) {
                // Find max-gain unlocked node (scan from top bucket down)
                int bestNode = -1;
                while (topBucket >= 0 && bucketHead[topBucket] == -1) topBucket--;
                if (topBucket < 0) break;

                for (int b = topBucket; b >= 0; b--) {
                    int v = bucketHead[b];
                    while (v >= 0) {
                        if (locked[v] != lockGen) {
                            int fromSide = side[v];
                            int newA = sizeA + (fromSide == 1 ? -1 : 1);
                            int newB = sizeB + (fromSide == 2 ? -1 : 1);
                            if (newA >= minSize && newB >= minSize) {
                                bestNode = v;
                                break;
                            }
                        }
                        v = fmN[v];
                    }
                    if (bestNode >= 0) break;
                }
                if (bestNode < 0) break;

                int nodeGain = gain[bestNode];

                // Lock and remove from bucket
                locked[bestNode] = lockGen;
                int b = nodeGain + bucketOffset;
                if (b < 0) b = 0;
                if (b >= numBuckets) b = numBuckets - 1;
                fmBucketRemove(bestNode, b, bucketHead, fmN, fmP);

                // Move node
                int fromSide = side[bestNode];
                int toSide = (fromSide == 1) ? 2 : 1;
                side[bestNode] = toSide;
                if (fromSide == 1) { sizeA--; sizeB++; }
                else { sizeA++; sizeB--; }

                moveNode[step] = bestNode;
                moveFrom[step] = fromSide;

                cumGain += nodeGain;
                if (cumGain > bestCumGain) {
                    bestCumGain = cumGain;
                    bestStep = step;
                }

                // Update neighbours' gains
                for (int w : adj[bestNode]) {
                    if (!inSubGraph[w] || locked[w] == lockGen) continue;

                    int oldGain = gain[w];
                    int oldB = oldGain + bucketOffset;
                    if (oldB < 0) oldB = 0;
                    if (oldB >= numBuckets) oldB = numBuckets - 1;

                    // w on fromSide → lost internal, gained external → gain += 2
                    // w on toSide   → lost external, gained internal → gain -= 2
                    if (side[w] == fromSide) gain[w] += 2;
                    else gain[w] -= 2;

                    int newB = gain[w] + bucketOffset;
                    if (newB < 0) newB = 0;
                    if (newB >= numBuckets) newB = numBuckets - 1;

                    if (oldB != newB) {
                        fmBucketRemove(w, oldB, bucketHead, fmN, fmP);
                        fmBucketInsert(w, newB, bucketHead, fmN, fmP);
                        if (newB > topBucket) topBucket = newB;
                    }
                }

                step++;
            }

            // Roll back to best position
            if (bestCumGain > 0) {
                improved = true;
                for (int s = step - 1; s > bestStep; s--) {
                    int v = moveNode[s];
                    side[v] = moveFrom[s];
                    if (moveFrom[s] == 1) { sizeA++; sizeB--; }
                    else { sizeA--; sizeB++; }
                }
            } else {
                // Roll back all moves
                for (int s = step - 1; s >= 0; s--) {
                    int v = moveNode[s];
                    side[v] = moveFrom[s];
                    if (moveFrom[s] == 1) { sizeA++; sizeB--; }
                    else { sizeA--; sizeB++; }
                }
            }
        }

        // Extract separator from the improved bipartition.
        int[][] extracted = extractSeparatorFromBipartition(allNodes, totalN, adj, side);

        // If bipartition extraction produced an invalid separator, try max-flow
        // on the FM-improved bipartition to get a guaranteed-valid result.
        if (!isValidSeparator(extracted, adj) && totalN >= MAXFLOW_MIN_SIZE) {
            int countA = 0, countB = 0;
            for (int i = 0; i < totalN; i++) {
                if (side[allNodes[i]] == 1) countA++;
                else countB++;
            }
            if (countA > 0 && countB > 0) {
                int[] sA = new int[countA];
                int[] sB = new int[countB];
                int ai = 0, bi = 0;
                for (int i = 0; i < totalN; i++) {
                    if (side[allNodes[i]] == 1) sA[ai++] = allNodes[i];
                    else sB[bi++] = allNodes[i];
                }
                int[][] mfResult = maxFlowRefineBipartition(sA, sB, adj);
                if (mfResult != null && isValidSeparator(mfResult, adj)) {
                    return mfResult;
                }
            }
        }

        return extracted;
    }

    /** Insert node v at head of bucket b. */
    private void fmBucketInsert(int v, int b, int[] bucketHead, int[] next, int[] prev) {
        prev[v] = -1;
        next[v] = bucketHead[b];
        if (bucketHead[b] >= 0) prev[bucketHead[b]] = v;
        bucketHead[b] = v;
    }

    /** Remove node v from bucket b. */
    private void fmBucketRemove(int v, int b, int[] bucketHead, int[] next, int[] prev) {
        if (prev[v] >= 0) next[prev[v]] = next[v];
        else bucketHead[b] = next[v];
        if (next[v] >= 0) prev[next[v]] = prev[v];
        next[v] = -1;
        prev[v] = -1;
    }

    /**
     * Extract a one-sided vertex separator from a bipartition (side 1 / side 2).
     * Tries both sides as separator source, picks the better after thinning.
     */
    private int[][] extractSeparatorFromBipartition(int[] allNodes, int totalN,
                                                     int[][] adj, int[] side) {
        // Find boundary nodes on each side
        int genB = ++scratchBoundaryGen;
        int[] boundaryMark = scratchBoundary;
        int boundaryACount = 0, boundaryBCount = 0;

        for (int i = 0; i < totalN; i++) {
            int node = allNodes[i];
            int mySide = side[node];
            for (int w : adj[node]) {
                if (!inSubGraph[w]) continue;
                if (side[w] != 0 && side[w] != mySide) {
                    if (boundaryMark[node] != genB) {
                        boundaryMark[node] = genB;
                        if (mySide == 1) boundaryACount++;
                        else boundaryBCount++;
                    }
                    break;
                }
            }
        }

        if (boundaryACount == 0 && boundaryBCount == 0) {
            // No boundary — return trivial split
            int half = totalN / 2;
            int[] pA = Arrays.copyOfRange(allNodes, 0, half);
            int[] sepArr = new int[]{allNodes[half]};
            int[] pB = Arrays.copyOfRange(allNodes, half + 1, totalN);
            return new int[][]{pA, sepArr, pB};
        }

        int[][] bestSideResult = null;
        long bestSideScore = Long.MAX_VALUE;

        for (int sepSide : new int[]{1, 2}) {
            int sepCount = (sepSide == 1) ? boundaryACount : boundaryBCount;
            if (sepCount == 0 || sepCount >= totalN - 1) continue;

            int countA = 0, countB = 0;
            for (int i = 0; i < totalN; i++) {
                int node = allNodes[i];
                boolean isSep = (boundaryMark[node] == genB && side[node] == sepSide);
                if (isSep) continue;
                if (side[node] == 1) countA++;
                else countB++;
            }
            if (countA == 0 || countB == 0) continue;

            int[] pA = new int[countA];
            int[] sep = new int[sepCount];
            int[] pB = new int[countB];
            int ia = 0, is = 0, ib = 0;
            for (int i = 0; i < totalN; i++) {
                int node = allNodes[i];
                boolean isSep = (boundaryMark[node] == genB && side[node] == sepSide);
                if (isSep) {
                    sep[is++] = node;
                } else if (side[node] == 1) {
                    pA[ia++] = node;
                } else {
                    pB[ib++] = node;
                }
            }

            int[][] candidate = new int[][]{pA, sep, pB};
            if (sep.length > 1) {
                candidate = thinSeparator(candidate, adj);
            }

            int thinnedBalance = Math.abs(candidate[0].length - candidate[2].length);
            long thinnedScore = scoreSeparator(candidate[1], thinnedBalance, adj);

            if (thinnedScore < bestSideScore) {
                bestSideScore = thinnedScore;
                bestSideResult = candidate;
            }
        }

        return bestSideResult != null ? bestSideResult :
                new int[][]{Arrays.copyOfRange(allNodes, 0, totalN / 2),
                            new int[]{allNodes[totalN / 2]},
                            Arrays.copyOfRange(allNodes, totalN / 2 + 1, totalN)};
    }

    // ========================= Max-Flow / Min-Cut ==============================

    /**
     * Find the minimum vertex separator using Dinic's algorithm on a node-split
     * flow network with a <b>wide cuttable region</b>.
     *
     * <p>Allows cutting any node within the given BFS depth from the separator.
     *
     * @param depth BFS depth for cuttable region
     * @return improved [partA, separator, partB], or null if no improvement
     */
    private int[][] maxFlowRefineWideDepth(int[][] result, int[][] adj, int depth) {
        int[] partA = result[0];
        int[] sep   = result[1];
        int[] partB = result[2];

        int totalN = partA.length + sep.length + partB.length;
        if (totalN < MAXFLOW_MIN_SIZE || sep.length <= 1) return null;

        // --- Mark which nodes are "cuttable" (capacity 1) ---
        // Start from separator nodes, BFS outward MAXFLOW_BORDER_DEPTH hops
        int genCuttable = ++scratchBoundaryGen;
        int[] cuttableMark = scratchBoundary;
        for (int s : sep) cuttableMark[s] = genCuttable;

        // BFS to expand cuttable region
        int[] bfsQueue = new int[totalN];
        int[] bfsDist = new int[graph.nodeCount]; // distance from sep
        int qHead = 0, qTail = 0;
        for (int s : sep) {
            bfsQueue[qTail++] = s;
            bfsDist[s] = 1; // mark as visited (1-indexed distance)
        }
        while (qHead < qTail) {
            int u = bfsQueue[qHead++];
            int d = bfsDist[u];
            if (d > depth) continue;
            for (int w : adj[u]) {
                if (!inSubGraph[w] || bfsDist[w] != 0) continue;
                bfsDist[w] = d + 1;
                cuttableMark[w] = genCuttable;
                if (qTail < bfsQueue.length) bfsQueue[qTail++] = w;
            }
        }
        // Cleanup bfsDist
        for (int i = 0; i < qTail; i++) bfsDist[bfsQueue[i]] = 0;
        for (int s : sep) bfsDist[s] = 0;

        // --- Compact mapping: global node ID → compact 0..totalN-1 ---
        int[] nodeId = new int[totalN];
        int[] toCompact = new int[graph.nodeCount];
        int ci = 0;
        for (int v : partA) { nodeId[ci] = v; toCompact[v] = ci + 1; ci++; }
        for (int v : sep)   { nodeId[ci] = v; toCompact[v] = ci + 1; ci++; }
        for (int v : partB) { nodeId[ci] = v; toCompact[v] = ci + 1; ci++; }

        int aLen = partA.length;
        int sLen = sep.length;

        // --- Build node-split flow network ---
        // Flow nodes: v_in=2*i, v_out=2*i+1, superS=2*totalN, superT=2*totalN+1
        int fN = 2 * totalN + 2;
        int superS = fN - 2;
        int superT = fN - 1;

        // Estimate edge count: internal(totalN) + original(~3*totalN) + source/sink(totalN)
        // Each edge creates 2 entries (forward + reverse), multiply by 2 for safety
        int maxEdges = (totalN + 6 * totalN + totalN) * 2;
        int[] eTo = new int[maxEdges];
        int[] eCap = new int[maxEdges];
        int[] eCount = {0};

        // Per-node adjacency
        int[][] fAdj = new int[fN][];
        int[] fAdjLen = new int[fN];

        int infCap = totalN + 1; // effectively infinite

        // Internal node-split edges: v_in → v_out
        // Cuttable nodes (in the border region) get capacity 1
        // Non-cuttable nodes (deep in partA or partB) get infinite capacity
        for (int i = 0; i < totalN; i++) {
            int globalId = nodeId[i];
            int cap = (cuttableMark[globalId] == genCuttable) ? 1 : infCap;
            flowAddEdge(2 * i, 2 * i + 1, cap, eTo, eCap, eCount, fAdj, fAdjLen);
        }

        // Original undirected edges: for each u↔v, add u_out→v_in and v_out→u_in
        for (int i = 0; i < totalN; i++) {
            int v = nodeId[i];
            for (int w : adj[v]) {
                int j = toCompact[w] - 1;
                if (j < 0 || j <= i) continue; // not in subgraph, or deduplicate
                flowAddEdge(2 * i + 1, 2 * j, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
                flowAddEdge(2 * j + 1, 2 * i, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
            }
        }

        // Source edges: superS → v_in for all non-cuttable A nodes (deep A)
        for (int i = 0; i < aLen; i++) {
            if (cuttableMark[nodeId[i]] != genCuttable) {
                flowAddEdge(superS, 2 * i, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
            }
        }

        // Sink edges: v_out → superT for all non-cuttable B nodes (deep B)
        for (int i = aLen + sLen; i < totalN; i++) {
            if (cuttableMark[nodeId[i]] != genCuttable) {
                flowAddEdge(2 * i + 1, superT, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
            }
        }

        // If no source or no sink connected, fall back: connect all A/B
        boolean hasSource = false, hasSink = false;
        for (int i = 0; i < aLen; i++) {
            if (cuttableMark[nodeId[i]] != genCuttable) { hasSource = true; break; }
        }
        for (int i = aLen + sLen; i < totalN; i++) {
            if (cuttableMark[nodeId[i]] != genCuttable) { hasSink = true; break; }
        }
        if (!hasSource) {
            for (int i = 0; i < aLen; i++)
                flowAddEdge(superS, 2 * i, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
        }
        if (!hasSink) {
            for (int i = aLen + sLen; i < totalN; i++)
                flowAddEdge(2 * i + 1, superT, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
        }

        // --- Dinic's algorithm ---
        int[] level = new int[fN];
        int[] iter = new int[fN];
        int[] queue = new int[fN];

        while (true) {
            // BFS to build level graph
            Arrays.fill(level, -1);
            level[superS] = 0;
            int bfsH = 0, bfsT = 0;
            queue[bfsT++] = superS;

            while (bfsH < bfsT) {
                int u = queue[bfsH++];
                int len = fAdjLen[u];
                int[] edges = fAdj[u];
                for (int ei = 0; ei < len; ei++) {
                    int e = edges[ei];
                    int w = eTo[e];
                    if (level[w] < 0 && eCap[e] > 0) {
                        level[w] = level[u] + 1;
                        queue[bfsT++] = w;
                    }
                }
            }

            if (level[superT] < 0) break; // no augmenting path

            // DFS to find blocking flows
            Arrays.fill(iter, 0);
            while (dinicDFS(superS, superT, infCap, level, iter, eTo, eCap, fAdj, fAdjLen) > 0) {
                // continue pushing
            }
        }

        // --- Extract min-cut: reachable from source in residual graph ---
        boolean[] reachable = new boolean[fN];
        reachable[superS] = true;
        {
            int rH = 0, rT = 0;
            queue[rT++] = superS;
            while (rH < rT) {
                int u = queue[rH++];
                int len = fAdjLen[u];
                int[] edges = fAdj[u];
                for (int ei = 0; ei < len; ei++) {
                    int e = edges[ei];
                    int w = eTo[e];
                    if (!reachable[w] && eCap[e] > 0) {
                        reachable[w] = true;
                        queue[rT++] = w;
                    }
                }
            }
        }

        // Min-cut: nodes where v_in is reachable but v_out is not
        int newSepCount = 0;
        int newACount = 0, newBCount = 0;
        for (int i = 0; i < totalN; i++) {
            if (reachable[2 * i] && !reachable[2 * i + 1]) {
                newSepCount++;
            } else if (reachable[2 * i]) {
                newACount++;
            } else {
                newBCount++;
            }
        }

        // Cleanup compact mapping
        for (int i = 0; i < totalN; i++) toCompact[nodeId[i]] = 0;

        // Validate: must have non-empty partitions
        if (newSepCount == 0 || newACount == 0 || newBCount == 0) {
            return null;
        }

        // Build result arrays
        int[] newSep = new int[newSepCount];
        int[] newA = new int[newACount];
        int[] newB = new int[newBCount];
        int ai = 0, si = 0, bi = 0;
        for (int i = 0; i < totalN; i++) {
            if (reachable[2 * i] && !reachable[2 * i + 1]) {
                newSep[si++] = nodeId[i];
            } else if (reachable[2 * i]) {
                newA[ai++] = nodeId[i];
            } else {
                newB[bi++] = nodeId[i];
            }
        }

        int[][] mfResult = new int[][]{newA, newSep, newB};
        if (newSep.length > 1) {
            mfResult = thinSeparator(mfResult, adj);
        }
        return mfResult;
    }

    /**
     * Add a directed edge from → to with given capacity, plus a reverse edge
     * (capacity 0) for the residual graph.  Edges are stored in pairs so that
     * the reverse of edge e is at index e^1.
     */
    private static void flowAddEdge(int from, int to, int cap,
                                     int[] eTo, int[] eCap, int[] eCount,
                                     int[][] fAdj, int[] fAdjLen) {
        int e = eCount[0];
        eTo[e] = to;       eCap[e] = cap;
        eTo[e + 1] = from; eCap[e + 1] = 0;
        flowAddToAdj(from, e, fAdj, fAdjLen);
        flowAddToAdj(to, e + 1, fAdj, fAdjLen);
        eCount[0] = e + 2;
    }

    private static void flowAddToAdj(int node, int edgeIdx, int[][] fAdj, int[] fAdjLen) {
        int len = fAdjLen[node];
        if (fAdj[node] == null) {
            fAdj[node] = new int[4];
        } else if (len == fAdj[node].length) {
            fAdj[node] = Arrays.copyOf(fAdj[node], len * 2);
        }
        fAdj[node][len] = edgeIdx;
        fAdjLen[node] = len + 1;
    }

    /** Dinic's DFS: find a blocking flow along shortest augmenting paths. */
    private static int dinicDFS(int u, int sink, int pushed,
                                 int[] level, int[] iter,
                                 int[] eTo, int[] eCap,
                                 int[][] fAdj, int[] fAdjLen) {
        if (u == sink) return pushed;
        int len = fAdjLen[u];
        int[] edges = fAdj[u];
        for (; iter[u] < len; iter[u]++) {
            int e = edges[iter[u]];
            int v = eTo[e];
            if (level[v] != level[u] + 1 || eCap[e] <= 0) continue;
            int d = dinicDFS(v, sink, Math.min(pushed, eCap[e]),
                             level, iter, eTo, eCap, fAdj, fAdjLen);
            if (d > 0) {
                eCap[e] -= d;
                eCap[e ^ 1] += d;
                return d;
            }
        }
        return 0;
    }

    /**
     * Find minimum vertex separator between sideA and sideB (pure bipartition,
     * no initial separator) using Dinic's max-flow.  All non-source/sink nodes
     * have capacity 1 in the node-split network.
     */
    private int[][] maxFlowRefineBipartition(int[] sideA, int[] sideB, int[][] adj) {
        int totalN = sideA.length + sideB.length;
        if (totalN < 10) return null;

        int[] nodeId = new int[totalN];
        int[] toCompact = new int[graph.nodeCount];
        int ci = 0;
        for (int v : sideA) { nodeId[ci] = v; toCompact[v] = ci + 1; ci++; }
        for (int v : sideB) { nodeId[ci] = v; toCompact[v] = ci + 1; ci++; }

        int aLen = sideA.length;

        int fN = 2 * totalN + 2;
        int superS = fN - 2;
        int superT = fN - 1;
        int infCap = totalN + 1;

        int maxEdges = (totalN + 6 * totalN + totalN) * 2;
        int[] eTo = new int[maxEdges];
        int[] eCap = new int[maxEdges];
        int[] eCount = {0};
        int[][] fAdj = new int[fN][];
        int[] fAdjLen = new int[fN];

        // Internal node-split: all nodes have capacity 1
        // (any can be part of the separator)
        for (int i = 0; i < totalN; i++) {
            flowAddEdge(2 * i, 2 * i + 1, 1, eTo, eCap, eCount, fAdj, fAdjLen);
        }

        // Original edges
        for (int i = 0; i < totalN; i++) {
            int v = nodeId[i];
            for (int w : adj[v]) {
                int j = toCompact[w] - 1;
                if (j < 0 || j <= i) continue;
                flowAddEdge(2 * i + 1, 2 * j, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
                flowAddEdge(2 * j + 1, 2 * i, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
            }
        }

        // Source → A nodes (extra capacity to avoid cutting source nodes)
        for (int i = 0; i < aLen; i++) {
            flowAddEdge(superS, 2 * i, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
            // Also boost A node internal capacity so they can't be cut
            eCap[i * 2] = infCap; // the v_in→v_out edge for node i
        }

        // B nodes → sink (extra capacity)
        for (int i = aLen; i < totalN; i++) {
            flowAddEdge(2 * i + 1, superT, infCap, eTo, eCap, eCount, fAdj, fAdjLen);
            eCap[i * 2] = infCap; // boost B node internal capacity
        }

        // Run Dinic's
        int[] level = new int[fN];
        int[] iter = new int[fN];
        int[] queue = new int[fN];

        while (true) {
            Arrays.fill(level, -1);
            level[superS] = 0;
            int qHead = 0, qTail = 0;
            queue[qTail++] = superS;
            while (qHead < qTail) {
                int u = queue[qHead++];
                int len = fAdjLen[u];
                int[] edges = fAdj[u];
                for (int ei = 0; ei < len; ei++) {
                    int e = edges[ei];
                    int w = eTo[e];
                    if (level[w] < 0 && eCap[e] > 0) {
                        level[w] = level[u] + 1;
                        queue[qTail++] = w;
                    }
                }
            }
            if (level[superT] < 0) break;
            Arrays.fill(iter, 0);
            while (dinicDFS(superS, superT, infCap, level, iter, eTo, eCap, fAdj, fAdjLen) > 0) {}
        }

        // Extract min-cut
        boolean[] reachable = new boolean[fN];
        reachable[superS] = true;
        int qHead = 0, qTail = 0;
        queue[qTail++] = superS;
        while (qHead < qTail) {
            int u = queue[qHead++];
            int len = fAdjLen[u];
            int[] edges = fAdj[u];
            for (int ei = 0; ei < len; ei++) {
                int e = edges[ei];
                int w = eTo[e];
                if (!reachable[w] && eCap[e] > 0) {
                    reachable[w] = true;
                    queue[qTail++] = w;
                }
            }
        }

        int newSepCount = 0, newACount = 0, newBCount = 0;
        for (int i = 0; i < totalN; i++) {
            if (reachable[2 * i] && !reachable[2 * i + 1]) newSepCount++;
            else if (reachable[2 * i]) newACount++;
            else newBCount++;
        }

        for (int i = 0; i < totalN; i++) toCompact[nodeId[i]] = 0;

        if (newSepCount == 0 || newACount == 0 || newBCount == 0) return null;

        int[] newSep = new int[newSepCount];
        int[] newA = new int[newACount];
        int[] newB = new int[newBCount];
        int ai = 0, si = 0, bi = 0;
        for (int i = 0; i < totalN; i++) {
            if (reachable[2 * i] && !reachable[2 * i + 1]) newSep[si++] = nodeId[i];
            else if (reachable[2 * i]) newA[ai++] = nodeId[i];
            else newB[bi++] = nodeId[i];
        }

        int[][] mfResult = new int[][]{newA, newSep, newB};
        if (newSep.length > 1) mfResult = thinSeparator(mfResult, adj);
        return mfResult;
    }

    /**
     * Split ratios to try for each projection direction.
     * On irregular road networks, the optimal cut is often NOT at the geometric
     * median.  Trying multiple split points finds "natural boundaries" (highways,
     * rivers, etc.) with far fewer boundary edges, dramatically reducing separator
     * size and thus shortcut count.
     *
     * <p>23 ratios × 16 directions = 368 candidates per recursion level.
     * The extra cost is negligible compared to contraction time.
     */
    private static final double[] SPLIT_RATIOS = {
        0.25, 0.28, 0.30, 0.33, 0.36, 0.38, 0.40, 0.42, 0.44, 0.46, 0.48,
        0.50, 0.52, 0.54, 0.56, 0.58, 0.60, 0.62, 0.64, 0.67, 0.70, 0.72, 0.75
    };

    /**
     * Try a single projection direction with <b>multiple split ratios</b>,
     * returning the best one-sided vertex separator found.
     *
     * <p>For each split ratio, the sorted projection is divided and a one-sided
     * vertex separator is extracted from the smaller boundary side.
     * The split with the best (lowest) score is returned.
     *
     * <p>Uses the shared {@link #inSubGraph} membership set (must be populated
     * by caller).
     */
    private int[][] tryDirection(int[] subNodes, int[][] adj, double dx, double dy) {
        int n = subNodes.length;
        if (n < 3) return null;

        // Project onto direction (once per direction, reused for all split ratios)
        double[] projections = new double[n];
        for (int i = 0; i < n; i++) {
            projections[i] = nodeX[subNodes[i]] * dx + nodeY[subNodes[i]] * dy;
        }
        int[] sortedIdx = sortByProjection(projections, n);

        int[][] bestResult = null;
        long bestScore = Long.MAX_VALUE;

        // Try each split ratio
        for (double ratio : SPLIT_RATIOS) {
            int splitAt = Math.max(1, Math.min(n - 1, (int) (n * ratio)));
            int[][] result = trySplitAt(subNodes, adj, sortedIdx, n, splitAt);
            if (result != null) {
                int balance = Math.abs(result[0].length - result[2].length);
                long score = scoreSimple(result[1], balance);
                if (score < bestScore) {
                    bestScore = score;
                    bestResult = result;
                }
            }
        }

        return bestResult;
    }

    /**
     * Try splitting the sorted projection at a specific index, extracting a
     * one-sided vertex separator from the smaller boundary side.
     *
     * <p>Uses the shared {@link #inSubGraph} membership set.
     *
     * @param subNodes  nodes in the sub-graph
     * @param adj       symmetric adjacency lists
     * @param sortedIdx projection-sorted indices into subNodes
     * @param n         number of nodes
     * @param splitAt   number of nodes on side A
     * @return [partA, separator, partB] or null if the split is degenerate
     */
    private int[][] trySplitAt(int[] subNodes, int[][] adj, int[] sortedIdx,
                                int n, int splitAt) {
        int[] side = scratchSide;

        // Reset only the nodes we're using (avoid full Arrays.fill)
        for (int i = 0; i < n; i++) side[subNodes[sortedIdx[i]]] = 0;

        for (int i = 0; i < splitAt; i++) {
            side[subNodes[sortedIdx[i]]] = 1; // side A
        }
        for (int i = splitAt; i < n; i++) {
            side[subNodes[sortedIdx[i]]] = 2; // side B
        }

        // Count boundary nodes on each side separately.
        // Use local arrays to avoid allocating boolean[] per call.
        int boundaryACount = 0, boundaryBCount = 0;
        // We need to track which nodes are boundary. Reuse a generation-stamped approach
        // to avoid allocating boolean arrays.
        int[] boundaryMark = scratchBoundary;
        int gen = ++scratchBoundaryGen;

        for (int idx = 0; idx < n; idx++) {
            int node = subNodes[idx];
            int mySide = side[node];
            for (int w : adj[node]) {
                if (!inSubGraph[w]) continue;
                if (side[w] != 0 && side[w] != mySide) {
                    if (boundaryMark[node] != gen) {
                        boundaryMark[node] = gen;
                        if (mySide == 1) boundaryACount++;
                        else boundaryBCount++;
                    }
                    break;
                }
            }
        }

        if (boundaryACount == 0 && boundaryBCount == 0) return null;

        // Try BOTH sides as separator and return the better one after thinning.
        int[][] bestSideResult = null;
        long bestSideScore = Long.MAX_VALUE;

        for (int sepSide : new int[]{1, 2}) {
            int sepCount = (sepSide == 1) ? boundaryACount : boundaryBCount;
            if (sepCount == 0 || sepCount >= n - 1) continue;

            int countA = 0, countB = 0;
            for (int idx = 0; idx < n; idx++) {
                int node = subNodes[idx];
                boolean isSep = (boundaryMark[node] == gen && side[node] == sepSide);
                if (isSep) continue;
                if (side[node] == 1) countA++;
                else countB++;
            }
            if (countA == 0 || countB == 0) continue;

            int[] pA = new int[countA];
            int[] sep = new int[sepCount];
            int[] pB = new int[countB];
            int ia2 = 0, is2 = 0, ib2 = 0;
            for (int idx = 0; idx < n; idx++) {
                int node = subNodes[idx];
                boolean isSep = (boundaryMark[node] == gen && side[node] == sepSide);
                if (isSep) {
                    sep[is2++] = node;
                } else if (side[node] == 1) {
                    pA[ia2++] = node;
                } else {
                    pB[ib2++] = node;
                }
            }

            // Apply thinning
            int[][] candidate = new int[][]{pA, sep, pB};
            if (sep.length > 1) {
                candidate = thinSeparator(candidate, adj);
            }

            int thinnedSepSize = candidate[1].length;
            int thinnedBalance = Math.abs(candidate[0].length - candidate[2].length);
            long thinnedScore = thinnedSepSize * 256L + thinnedBalance;

            if (thinnedScore < bestSideScore) {
                bestSideScore = thinnedScore;
                bestSideResult = candidate;
            }
        }

        return bestSideResult;
    }

    /**
     * Greedy separator thinning with iterative passes.
     *
     * <p>A separator node s can be safely removed (moved to P2) if NONE of its
     * neighbors are in P1 — it does not block any P1→P2 path.
     *
     * <p>Runs multiple passes: after removing some separator nodes, their former
     * separator neighbors may become removable too.  Iterates until fixpoint.
     */
    private int[][] thinSeparator(int[][] result, int[][] adj) {
        int[] partA = result[0];
        int[] separator = result[1];
        int[] partB = result[2];

        if (separator.length <= 1) return result;

        boolean changed = true;
        while (changed && separator.length > 1) {
            changed = false;

            int genP1 = ++scratchBoundaryGen;
            int[] mark = scratchBoundary;

            for (int n : partA) mark[n] = genP1;

            boolean[] removable = new boolean[separator.length];
            int removeCount = 0;

            for (int i = 0; i < separator.length; i++) {
                int s = separator[i];
                boolean hasP1Neighbor = false;
                for (int w : adj[s]) {
                    if (mark[w] == genP1) {
                        hasP1Neighbor = true;
                        break;
                    }
                }
                if (!hasP1Neighbor) {
                    removable[i] = true;
                    removeCount++;
                }
            }

            if (removeCount == 0) break;
            if (removeCount == separator.length) break; // keep all — degenerate

            // Rebuild: removed separator nodes go to P2
            int[] newSep = new int[separator.length - removeCount];
            int[] newPartB = new int[partB.length + removeCount];
            System.arraycopy(partB, 0, newPartB, 0, partB.length);
            int si = 0, bi = partB.length;
            for (int i = 0; i < separator.length; i++) {
                if (removable[i]) {
                    newPartB[bi++] = separator[i];
                } else {
                    newSep[si++] = separator[i];
                }
            }

            separator = newSep;
            partB = newPartB;
            changed = true;
        }

        return new int[][]{partA, separator, partB};
    }

    /**
     * Sort indices [0..n) by the corresponding projection values.
     * Uses a merge-sort on primitive int[] to avoid boxing overhead.
     */
    private static int[] sortByProjection(double[] projections, int n) {
        int[] idx = new int[n];
        for (int i = 0; i < n; i++) idx[i] = i;
        int[] tmp = new int[n];
        mergeSort(idx, tmp, projections, 0, n);
        return idx;
    }

    private static void mergeSort(int[] arr, int[] tmp, double[] keys, int lo, int hi) {
        if (hi - lo <= 16) {
            // Insertion sort for small ranges
            for (int i = lo + 1; i < hi; i++) {
                int t = arr[i];
                double k = keys[t];
                int j = i - 1;
                while (j >= lo && keys[arr[j]] > k) {
                    arr[j + 1] = arr[j];
                    j--;
                }
                arr[j + 1] = t;
            }
            return;
        }
        int mid = (lo + hi) >>> 1;
        mergeSort(arr, tmp, keys, lo, mid);
        mergeSort(arr, tmp, keys, mid, hi);

        // Merge
        System.arraycopy(arr, lo, tmp, lo, hi - lo);
        int i = lo, j = mid, k = lo;
        while (i < mid && j < hi) {
            arr[k++] = (keys[tmp[i]] <= keys[tmp[j]]) ? tmp[i++] : tmp[j++];
        }
        while (i < mid) arr[k++] = tmp[i++];
        while (j < hi)  arr[k++] = tmp[j++];
    }

    private int[][] trivialSplit(int[] subNodes) {
        int half = subNodes.length / 2;
        int sepSize = Math.max(1, subNodes.length / 10);
        int partASize = half - sepSize / 2;
        if (partASize < 1) partASize = 1;
        int partBSize = subNodes.length - partASize - sepSize;
        if (partBSize < 0) {
            partBSize = 0;
            sepSize = subNodes.length - partASize;
        }

        int[] partA = Arrays.copyOfRange(subNodes, 0, partASize);
        int[] separator = Arrays.copyOfRange(subNodes, partASize, partASize + sepSize);
        int[] partB = Arrays.copyOfRange(subNodes, partASize + sepSize, subNodes.length);
        return new int[][]{partA, separator, partB};
    }

    /**
     * Build symmetric (undirected) adjacency lists from the directed SpeedyGraph.
     */
    private int[][] buildSymmetricAdjacency() {
        int n = graph.nodeCount;

        // Count neighbors
        int[] degree = new int[n];
        SpeedyGraph.LinkIterator outLI = graph.getOutLinkIterator();
        for (int node = 0; node < n; node++) {
            outLI.reset(node);
            while (outLI.next()) {
                int to = outLI.getToNodeIndex();
                degree[node]++;
                degree[to]++;
            }
        }

        // Allocate
        int[][] adj = new int[n][];
        int[] pos = new int[n];
        for (int i = 0; i < n; i++) {
            adj[i] = new int[degree[i]];
        }

        // Fill
        for (int node = 0; node < n; node++) {
            outLI.reset(node);
            while (outLI.next()) {
                int to = outLI.getToNodeIndex();
                adj[node][pos[node]++] = to;
                adj[to][pos[to]++] = node;
            }
        }

        // Deduplicate (remove duplicate neighbor entries)
        for (int i = 0; i < n; i++) {
            if (pos[i] == 0) {
                adj[i] = new int[0];
                continue;
            }
            Arrays.sort(adj[i], 0, pos[i]);
            int unique = 1;
            for (int j = 1; j < pos[i]; j++) {
                if (adj[i][j] != adj[i][j - 1]) {
                    adj[i][unique++] = adj[i][j];
                }
            }
            adj[i] = Arrays.copyOf(adj[i], unique);
        }

        return adj;
    }
}
