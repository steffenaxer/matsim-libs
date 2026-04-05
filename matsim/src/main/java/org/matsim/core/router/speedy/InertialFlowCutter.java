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
 * coordinate-based inertial flow partitioning.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Project all nodes in the sub-graph onto several axis directions
 *       (horizontal, vertical, and two diagonals).</li>
 *   <li>For each direction, split nodes at the projection median into two halves.
 *       The direction with the best (smallest, most balanced) separator is used.</li>
 *   <li>The boundary is converted to a <b>one-sided vertex separator</b>: only
 *       nodes on the smaller boundary side become separator nodes.
 *       This is critical for grids where a naïve two-sided boundary doubles the
 *       separator size.</li>
 *   <li>The separator nodes receive the highest contraction levels.
 *       The two partitions are recursively dissected.</li>
 * </ol>
 *
 * <p>This eliminates witness searches during ordering and is dramatically faster
 * than priority-queue-based ordering for large networks.
 *
 * <p>References: Dibbelt et al. (2016) "Customizable Contraction Hierarchies",
 * Hamann &amp; Strasser (2018) "Graph Bisection with Pareto Optimization".
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
    private static final int MIN_PARTITION_SIZE = 3;

    private final SpeedyGraph graph;

    // Node coordinates (extracted once)
    private final double[] nodeX;
    private final double[] nodeY;

    // Reusable scratch arrays (sized to nodeCount)
    private final int[] scratchSide;
    private final int[] scratchBoundary;  // generation-stamped boundary markers
    private int scratchBoundaryGen = 0;

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
     * Find a separator for the given sub-graph using inertial flow.
     * Returns [partitionA, separator, partitionB].
     */
    private int[][] findSeparator(int[] subNodes, int[][] adj) {
        // Try 16 projection directions covering 22.5° increments.
        // More directions = better chance of finding natural graph boundaries
        // (highways, rivers, rail lines) that produce small separators.
        double[][] directions = {
            {1, 0}, {0, 1}, {1, 1}, {1, -1},        // 0°, 90°, 45°, 135°
            {2, 1}, {1, 2}, {2, -1}, {1, -2},        // ~27°, ~63°, ~153°, ~117°
            {3, 1}, {1, 3}, {3, -1}, {1, -3},        // ~18.4°, ~71.6°, ~161.6°, ~108.4°
            {4, 1}, {1, 4}, {4, -1}, {1, -4}         // ~14.0°, ~76.0°, ~166.0°, ~104.0°
        };

        int[][] bestResult = null;
        int bestScore = Integer.MAX_VALUE;

        for (double[] dir : directions) {
            int[][] result = tryDirection(subNodes, adj, dir[0], dir[1]);
            if (result != null) {
                int sepSize = result[1].length;
                int balance = Math.abs(result[0].length - result[2].length);
                // Weight separator size heavily: every extra separator node
                // causes O(degree²) shortcuts during contraction.
                int score = sepSize * 256 + balance;
                if (score < bestScore) {
                    bestScore = score;
                    bestResult = result;
                }
            }
        }

        if (bestResult == null) {
            return trivialSplit(subNodes);
        }

        return bestResult;
    }

    /**
     * Split ratios to try for each projection direction.
     * On irregular road networks, the optimal cut is often NOT at the geometric
     * median.  Trying multiple split points finds "natural boundaries" (highways,
     * rivers, etc.) with far fewer boundary edges, dramatically reducing separator
     * size and thus shortcut count.
     *
     * <p>11 ratios × 16 directions = 176 candidates per recursion level.
     * The extra cost is negligible compared to contraction time.
     */
    private static final double[] SPLIT_RATIOS = {
        0.30, 0.33, 0.36, 0.40, 0.44, 0.50, 0.56, 0.60, 0.64, 0.67, 0.70
    };

    /**
     * Try a single projection direction with <b>multiple split ratios</b>,
     * returning the best one-sided vertex separator found.
     *
     * <p>For each split ratio, the sorted projection is divided and a one-sided
     * vertex separator is extracted from the smaller boundary side.
     * The split with the best (lowest) score is returned.
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

        // Build membership set for fast lookup
        boolean[] inSubGraph = new boolean[graph.nodeCount];
        for (int node : subNodes) inSubGraph[node] = true;

        int[][] bestResult = null;
        int bestScore = Integer.MAX_VALUE;

        // Try each split ratio
        for (double ratio : SPLIT_RATIOS) {
            int splitAt = Math.max(1, Math.min(n - 1, (int) (n * ratio)));
            int[][] result = trySplitAt(subNodes, adj, sortedIdx, n, splitAt, inSubGraph);
            if (result != null) {
                int sepSize = result[1].length;
                int balance = Math.abs(result[0].length - result[2].length);
                int score = sepSize * 256 + balance;
                if (score < bestScore) {
                    bestScore = score;
                    bestResult = result;
                }
            }
        }

        // Clean up membership set
        for (int node : subNodes) inSubGraph[node] = false;

        return bestResult;
    }

    /**
     * Try splitting the sorted projection at a specific index, extracting a
     * one-sided vertex separator from the smaller boundary side.
     *
     * @param subNodes  nodes in the sub-graph
     * @param adj       symmetric adjacency lists
     * @param sortedIdx projection-sorted indices into subNodes
     * @param n         number of nodes
     * @param splitAt   number of nodes on side A
     * @param inSubGraph membership lookup (must be set for all subNodes)
     * @return [partA, separator, partB] or null if the split is degenerate
     */
    private int[][] trySplitAt(int[] subNodes, int[][] adj, int[] sortedIdx,
                                int n, int splitAt, boolean[] inSubGraph) {
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
        int bestSideScore = Integer.MAX_VALUE;

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
            int thinnedScore = thinnedSepSize * 8 + thinnedBalance;

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
