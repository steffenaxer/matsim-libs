package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Node;

/**
 * CH (Contraction Hierarchies) overlay graph built on top of a {@link SpeedyGraph}.
 *
 * <h3>Memory layout (CSR – Compressed Sparse Row)</h3>
 * <p>Upward out-edges and downward in-edges are stored in two separate, contiguous
 * {@code int[]} arrays ({@link #upEdges}, {@link #dnEdges}).  For each node the
 * offset into that array and the number of edges are stored in
 * {@link #upOff}/{@link #upLen} and {@link #dnOff}/{@link #dnLen}.
 * This gives O(1) random access per node and sequential memory access when
 * iterating a node's edges — far better cache behaviour than a linked list.
 *
 * <p>Per-edge data (2 ints each, packed tightly):
 * <pre>
 *   [0] toNode / fromNode index   (toNode for up-edges, fromNode for dn-edges)
 *   [1] global edge index         (for TTF/weight lookup and edge unpacking)
 * </pre>
 * Edge metadata (origLink, lowerEdge1/2) is stored in separate global arrays
 * indexed by the global edge index, keeping the CSR compact for the query hot path.
 *
 * <h3>Global edge index layout</h3>
 * <p>Global indices are assigned contiguously per CSR slot:
 * up-edges use indices {@code [0, upEdgeCount)}, down-edges use
 * {@code [upEdgeCount, upEdgeCount + dnEdgeCount)}.  This ensures that
 * edges of the same node are contiguous in the TTF array.
 *
 * <h3>Weight layout</h3>
 * <p>Edge weights are colocated in contiguous {@code double[]} arrays
 * ({@link #upWeights}, {@link #dnWeights}) indexed by edge slot, so that
 * iterating a node's edges reads weight from the same cache line as the
 * target node index.
 *
 * <h3>TTF layout (bin-major, flat contiguous)</h3>
 * <p>{@link #ttf} is a single {@code double[NUM_BINS * edgeCount]} array where
 * {@code ttf[bin * edgeCount + globalIdx]} is the travel time for edge
 * {@code globalIdx} departing in time bin {@code bin}.  The bin-major layout
 * gives sequential memory access when iterating a node's edges (constant bin,
 * contiguous globalIdx values), which is the dominant access pattern in the
 * forward CH query.
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHGraph {

    /** Ints per edge in the CSR edge arrays (toNode + globalIdx only). */
    static final int E_STRIDE = 2;
    static final int E_NODE   = 0; // toNode (up) or fromNode (dn)
    static final int E_GIDX   = 1; // global edge index

    final int nodeCount;

    // --- upward out-edges (used by forward search) ---
    final int   upEdgeCount;
    final int[] upOff;       // upOff[node] = start index in upEdges for this node
    final int[] upLen;       // upLen[node] = number of upward edges for this node
    final int[] upEdges;     // E_STRIDE ints per edge, packed
    final double[] upWeights;   // upWeights[slot] = edge weight, colocated for cache locality

    // --- downward in-edges (used by backward search) ---
    final int   dnEdgeCount;
    final int[] dnOff;
    final int[] dnLen;
    final int[] dnEdges;
    final double[] dnWeights;

    // --- per-edge (global index) data ---
    final int totalEdgeCount;       // = upEdgeCount + dnEdgeCount (every edge appears in exactly one list)
    final double[] edgeWeights;     // edgeWeights[globalIdx] (kept for customizer compatibility)
    final int[]    edgeOrigLink;    // originalLinkIndex per global edge
    final int[]    edgeLower1;      // lowerEdge1 per global edge
    final int[]    edgeLower2;      // lowerEdge2 per global edge

    // Time-dependent TTF – bin-major flat contiguous array.
    // ttf[bin * totalEdgeCount + globalIdx] = travel time (seconds).
    // Bin-major layout gives sequential access when iterating a node's edges
    // (constant bin, contiguous globalIdx values).
    double[] ttf;
    double[] minTTF;

    // Change detection for incremental TTF customization.
    // ttfHash[globalIdx] = sum of TTF bins; NaN until first customization.
    double[] ttfHash;

    // Topological processing order for customization.
    // Ensures lower (component) edges are processed before their parent shortcuts.
    final int[] customizeOrder;

    private final SpeedyGraph baseGraph;

    SpeedyCHGraph(SpeedyGraph baseGraph, int nodeCount,
                  int upEdgeCount, int[] upOff, int[] upLen, int[] upEdges, double[] upWeights,
                  int dnEdgeCount, int[] dnOff, int[] dnLen, int[] dnEdges, double[] dnWeights,
                  int totalEdgeCount, int[] edgeOrigLink, int[] edgeLower1, int[] edgeLower2,
                  int[] customizeOrder) {
        this.baseGraph      = baseGraph;
        this.nodeCount      = nodeCount;
        this.upEdgeCount    = upEdgeCount;
        this.upOff          = upOff;
        this.upLen          = upLen;
        this.upEdges        = upEdges;
        this.upWeights      = upWeights;
        this.dnEdgeCount    = dnEdgeCount;
        this.dnOff          = dnOff;
        this.dnLen          = dnLen;
        this.dnEdges        = dnEdges;
        this.dnWeights      = dnWeights;
        this.totalEdgeCount = totalEdgeCount;
        this.edgeOrigLink   = edgeOrigLink;
        this.edgeLower1     = edgeLower1;
        this.edgeLower2     = edgeLower2;
        this.customizeOrder = customizeOrder;
        this.edgeWeights    = new double[totalEdgeCount];
        // Pre-allocate bin-major flat TTF arrays.
        this.ttf    = new double[SpeedyCHTTFCustomizer.NUM_BINS * totalEdgeCount];
        this.minTTF = new double[totalEdgeCount];
        this.ttfHash = new double[totalEdgeCount];
        java.util.Arrays.fill(this.ttfHash, Double.NaN);
    }

    SpeedyGraph getBaseGraph() {
        return baseGraph;
    }

    Node getNode(int nodeIndex) {
        return baseGraph.getNode(nodeIndex);
    }
}
