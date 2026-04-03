package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Node;

/**
 * CH (Contraction Hierarchies) overlay graph built on top of a {@link SpeedyGraph}.
 *
 * <p>After contraction, edges are separated into:
 * <ul>
 *   <li><b>Upward edges</b> (level[from] &lt; level[to]): stored in the out-up adjacency of
 *       {@code fromNode}. Used by the <em>forward</em> CH search.</li>
 *   <li><b>Downward edges</b> (level[from] &gt; level[to]): stored in the down-in adjacency of
 *       {@code toNode}. Used by the <em>backward</em> CH search.</li>
 * </ul>
 *
 * <p>Edge data layout – {@value #EDGE_SIZE} ints per edge:
 * <pre>
 *   [0] next out-up  edge for fromNode  (linked-list pointer; -1 if last or downward edge)
 *   [1] next down-in edge for toNode    (linked-list pointer; -1 if last or upward edge)
 *   [2] fromNode index
 *   [3] toNode   index
 *   [4] originalLinkIndex  (&gt;=0 for real edges, -1 for shortcuts)
 *   [5] middleNode         (-1 for real edges)
 *   [6] lowerEdge1 index   (edge from→middle; -1 for real edges)
 *   [7] lowerEdge2 index   (edge middle→to;   -1 for real edges)
 * </pre>
 *
 * <p>Node data layout – {@value #NODE_SIZE} ints per node:
 * <pre>
 *   [0] first out-up  edge of this node (forward  search)
 *   [1] first down-in edge of this node (backward search)
 * </pre>
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHGraph {

    static final int NODE_SIZE = 2;
    static final int EDGE_SIZE = 8;

    final int nodeCount;
    final int edgeCount;
    final int[] nodeData;   // NODE_SIZE ints per node
    final int[] edgeData;   // EDGE_SIZE ints per edge
    final int[] nodeLevel;  // CH level per node (0 = least important, nodeCount-1 = most important)
    final double[] edgeWeights; // static lower-bound weights (= min TTF per edge after time-dep customization)

    // Time-dependent Travel Time Functions (TTF) – filled by SpeedyCHTTFCustomizer.
    // ttf[edgeIdx][binIdx] = travel time (seconds) for departure in time bin binIdx.
    double[][] ttf;    // null until SpeedyCHTTFCustomizer has run
    double[] minTTF;   // precomputed per-edge minimum over all time bins; null until computed

    private final SpeedyGraph baseGraph;

    SpeedyCHGraph(SpeedyGraph baseGraph, int nodeCount, int edgeCount,
                  int[] nodeData, int[] edgeData, int[] nodeLevel) {
        this.baseGraph = baseGraph;
        this.nodeCount = nodeCount;
        this.edgeCount = edgeCount;
        this.nodeData = nodeData;
        this.edgeData = edgeData;
        this.nodeLevel = nodeLevel;
        this.edgeWeights = new double[edgeCount];
        // Pre-allocate TTF arrays so customization can reuse them across iterations.
        this.ttf    = new double[edgeCount][SpeedyCHTTFCustomizer.NUM_BINS];
        this.minTTF = new double[edgeCount];
    }

    SpeedyGraph getBaseGraph() {
        return baseGraph;
    }

    /** Returns the original MATSim {@link Link} for a real edge, or {@code null} for a shortcut. */
    Link getLink(int edgeIndex) {
        int linkIdx = edgeData[edgeIndex * EDGE_SIZE + 4];
        if (linkIdx < 0) return null;
        return baseGraph.getLink(linkIdx);
    }

    Node getNode(int nodeIndex) {
        return baseGraph.getNode(nodeIndex);
    }

    /** Returns an iterator for upward out-edges of a node (used in forward search). */
    UpwardOutEdgeIterator getUpwardOutEdgeIterator() {
        return new UpwardOutEdgeIterator(this);
    }

    /** Returns an iterator for downward in-edges of a node (used in backward search). */
    DownwardInEdgeIterator getDownwardInEdgeIterator() {
        return new DownwardInEdgeIterator(this);
    }

    // -------------------------------------------------------------------------
    // Iterator implementations
    // -------------------------------------------------------------------------

    /**
     * Iterates over upward out-edges of a node: edges (node → w) where level[node] &lt; level[w].
     * Used by the <em>forward</em> CH search.
     */
    static final class UpwardOutEdgeIterator {
        private final SpeedyCHGraph chGraph;
        private int nodeIdx = -1;
        private int edgeIdx = -1;

        UpwardOutEdgeIterator(SpeedyCHGraph chGraph) {
            this.chGraph = chGraph;
        }

        void reset(int nodeIdx) {
            this.nodeIdx = nodeIdx;
            this.edgeIdx = -1;
        }

        boolean next() {
            if (nodeIdx < 0) return false;
            edgeIdx = (edgeIdx < 0)
                    ? chGraph.nodeData[nodeIdx * NODE_SIZE]
                    : chGraph.edgeData[edgeIdx * EDGE_SIZE];
            if (edgeIdx < 0) {
                nodeIdx = -1;
                return false;
            }
            return true;
        }

        int getEdgeIndex()         { return edgeIdx; }
        int getFromNode()          { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 2]; }
        int getToNode()            { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 3]; }
        double getWeight()         { return chGraph.edgeWeights[edgeIdx]; }
        int getOriginalLinkIndex() { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 4]; }
        int getMiddleNode()        { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 5]; }
        int getLowerEdge1()        { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 6]; }
        int getLowerEdge2()        { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 7]; }
    }

    /**
     * Iterates over downward in-edges of a node: edges (y → node) where level[y] &gt; level[node].
     * Used by the <em>backward</em> CH search (traversed in reverse: node ← y).
     */
    static final class DownwardInEdgeIterator {
        private final SpeedyCHGraph chGraph;
        private int nodeIdx = -1;
        private int edgeIdx = -1;

        DownwardInEdgeIterator(SpeedyCHGraph chGraph) {
            this.chGraph = chGraph;
        }

        void reset(int nodeIdx) {
            this.nodeIdx = nodeIdx;
            this.edgeIdx = -1;
        }

        boolean next() {
            if (nodeIdx < 0) return false;
            edgeIdx = (edgeIdx < 0)
                    ? chGraph.nodeData[nodeIdx * NODE_SIZE + 1]
                    : chGraph.edgeData[edgeIdx * EDGE_SIZE + 1];
            if (edgeIdx < 0) {
                nodeIdx = -1;
                return false;
            }
            return true;
        }

        int getEdgeIndex()         { return edgeIdx; }
        /** The higher-level node that points to {@code nodeIdx} via this downward edge. */
        int getFromNode()          { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 2]; }
        int getToNode()            { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 3]; }
        double getWeight()         { return chGraph.edgeWeights[edgeIdx]; }
        int getOriginalLinkIndex() { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 4]; }
        int getMiddleNode()        { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 5]; }
        int getLowerEdge1()        { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 6]; }
        int getLowerEdge2()        { return chGraph.edgeData[edgeIdx * EDGE_SIZE + 7]; }
    }
}
