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
 * <p>Per-edge data (5 ints each, packed tightly):
 * <pre>
 *   [0] toNode / fromNode index   (toNode for up-edges, fromNode for dn-edges)
 *   [1] originalLinkIndex         (≥0 for real edges, -1 for shortcuts)
 *   [2] lowerEdge1 index          (-1 for real edges)
 *   [3] lowerEdge2 index          (-1 for real edges)
 *   [4] global edge index         (for TTF/weight lookup)
 * </pre>
 *
 * <h3>Weight layout</h3>
 * <p>Edge weights are colocated in contiguous {@code double[]} arrays
 * ({@link #upWeights}, {@link #dnWeights}) indexed by edge slot, so that
 * iterating a node's edges reads weight from the same cache line as the
 * target node index.
 *
 * <h3>TTF layout (flat contiguous)</h3>
 * <p>{@link #ttf} is a single {@code double[edgeCount * NUM_BINS]} array where
 * {@code ttf[e * NUM_BINS + k]} is the travel time for edge {@code e} departing
 * in time bin {@code k}. This avoids the pointer chase of {@code double[][]}.
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHGraph {

    /** Ints per edge in the CSR edge arrays. */
    static final int E_STRIDE = 5;
    static final int E_NODE   = 0; // toNode (up) or fromNode (dn)
    static final int E_ORIG   = 1; // originalLinkIndex
    static final int E_LOW1   = 2; // lowerEdge1
    static final int E_LOW2   = 3; // lowerEdge2
    static final int E_GIDX   = 4; // global edge index

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

    // Time-dependent TTF – flat contiguous array.
    // ttf[globalIdx * NUM_BINS + bin] = travel time (seconds).
    double[] ttf;
    double[] minTTF;

    private final SpeedyGraph baseGraph;

    SpeedyCHGraph(SpeedyGraph baseGraph, int nodeCount,
                  int upEdgeCount, int[] upOff, int[] upLen, int[] upEdges, double[] upWeights,
                  int dnEdgeCount, int[] dnOff, int[] dnLen, int[] dnEdges, double[] dnWeights,
                  int totalEdgeCount, int[] edgeOrigLink, int[] edgeLower1, int[] edgeLower2) {
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
        this.edgeWeights    = new double[totalEdgeCount];
        // Pre-allocate flat TTF arrays.
        this.ttf    = new double[totalEdgeCount * SpeedyCHTTFCustomizer.NUM_BINS];
        this.minTTF = new double[totalEdgeCount];
    }

    SpeedyGraph getBaseGraph() {
        return baseGraph;
    }

    Node getNode(int nodeIndex) {
        return baseGraph.getNode(nodeIndex);
    }
}
