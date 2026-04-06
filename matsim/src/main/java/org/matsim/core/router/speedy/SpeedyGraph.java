package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.network.turnRestrictions.TurnRestrictionsContext;

import java.util.Optional;

/**
 * Implements a highly optimized data structure for representing a MATSim network. Optimized to use as little memory as possible, and thus to fit as much memory as possible into CPU caches for high
 * performance.
 * <p>
 * Inspired by GraphHopper's Graph data structure as described in https://github.com/graphhopper/graphhopper/blob/master/docs/core/technical.md. GraphHopper uses bi-directional links, while MATSim
 * uses uni-directional links. Thus, instead of having nodeA and nodeB, we always have from- and to-node. This implies that one `node-row` requires two link-ids: one for the first in-link, and one for
 * the first out-link.
 * <p>
 * We use simple int-arrays (int[]) to store the data. This should provide fast and thread-safe read-only access, but limits the number of nodes and links in the network to (Integer.MAX_VALUE/2 =
 * 1.073.741.823) nodes and (Integer.MAX_VALUE/6 = 357.913.941) links. I hope that for the foreseeable future, these limits are high enough.
 * <p>
 * <h3>Optional CSR (Compressed Sparse Row) optimization</h3>
 * <p>The default adjacency representation uses linked lists stored in {@code linkData}, where
 * edges for a node may be scattered across the array.  Call {@link #buildCSR()} to build an
 * optional CSR (Compressed Sparse Row) representation that stores all out-/in-edges for each
 * node contiguously with colocated edge data (linkIdx, toNode/fromNode, length, freespeed).
 * When CSR is enabled, {@link #getOutLinkIterator()} and {@link #getInLinkIterator()} return
 * CSR-based iterators that deliver significantly better cache locality during Dijkstra traversal.
 * <p>
 * CSR adds ~20 bytes per link of additional memory but provides measurable speedups on larger
 * networks where cache misses from linked-list pointer chasing become a bottleneck.
 * <p>
 * This class is thread-safe, allowing a single graph to be used by multiple threads.
 *
 * @author mrieser
 */
public class SpeedyGraph {

    /*
     * memory consumption (default linked-list mode):
     * - nodeData:
     *   - 1 int: next out-link index
     *   - 1 int: next in-link index
     *   = total 2 int = 8 bytes per node
     * - linkData
     *   - 1 int: next out-link index for from-node
     *   - 1 int: next in-link index for to-node
     *   - 1 int: from-node index
     *   - 1 int: to-node index
     *   - 1 int: link length * 100
     *   - 1 int: freespeed-traveltime * 100
     *   = total 6 int per link = 24 bytes per link
     * - links
     *   - 1 object-pointer per link
     *   = 1 int or 1 long per link (depending on 32 or 64bit JVM) = 4 or 8 bytes per link
     *
     *   So, a network-graph with 1 Mio nodes and 2 Mio links should consume between 64 and 72 MB RAM only.
     *
     * Additional memory with CSR enabled:
     * - outOff, outLen, inOff, inLen: 4 int arrays × nodeCount = 16 bytes per node
     * - outEdges, inEdges: CSR_STRIDE ints per link (× 2 for out+in) = 20 bytes per link
     *   (each link appears once in outEdges for its fromNode and once in inEdges for its toNode)
     */

    final static int NODE_SIZE = 2;
    final static int LINK_SIZE = 6;

    /** Ints per edge in the CSR edge arrays. */
    static final int CSR_STRIDE = 5;
    /** Offsets within one CSR edge slot. */
    static final int CSR_LINK_IDX   = 0;
    static final int CSR_TO_NODE    = 1;  // toNode for out-edges, fromNode for in-edges
    static final int CSR_FROM_NODE  = 2;  // fromNode for out-edges, toNode for in-edges
    static final int CSR_LENGTH     = 3;  // link.getLength() * 100
    static final int CSR_FREESPEED  = 4;  // freespeed travel time * 100

    final int nodeCount;
    final int linkCount;
    private final int[] nodeData;
    private final int[] linkData;
    private final Link[] links;
    private final Node[] nodes;
    private final TurnRestrictionsContext turnRestrictions;

    // --- Optional CSR arrays (null until buildCSR() is called) ---
    private int[] outOff;       // outOff[node] = start index in outEdges
    private int[] outLen;       // outLen[node] = number of out-edges
    private int[] outEdges;     // CSR_STRIDE ints per edge, contiguous per node
    private int[] inOff;        // inOff[node]  = start index in inEdges
    private int[] inLen;        // inLen[node]  = number of in-edges
    private int[] inEdges;      // CSR_STRIDE ints per edge, contiguous per node

	SpeedyGraph(int[] nodeData, int[] linkData, Node[] nodes, Link[] links, TurnRestrictionsContext turnRestrictions) {
		this.nodeData = nodeData;
		this.linkData = linkData;
		this.nodes = nodes;
		this.links = links;
		this.nodeCount = this.nodes.length;
		this.linkCount = this.links.length;
		this.turnRestrictions = turnRestrictions;
	}

    /**
     * Builds the CSR (Compressed Sparse Row) representation for this graph.
     *
     * <p>After this call, {@link #getOutLinkIterator()} and {@link #getInLinkIterator()}
     * will return CSR-based iterators with better cache locality.  This method is
     * idempotent and thread-safe (builds only once).
     *
     * <p>The CSR arrays store all out-/in-edges for each node contiguously with
     * colocated edge data (linkIdx, toNode/fromNode, length, freespeed travel time),
     * eliminating the linked-list pointer chasing of the default representation.
     */
    public synchronized void buildCSR() {
        if (this.outOff != null) {
            return; // already built
        }

        int[] oOff = new int[nodeCount];
        int[] oLen = new int[nodeCount];
        int[] iOff = new int[nodeCount];
        int[] iLen = new int[nodeCount];

        // Pass 1: count edges per node
        for (int n = 0; n < nodeCount; n++) {
            int linkIdx = nodeData[n * NODE_SIZE];
            while (linkIdx >= 0) {
                oLen[n]++;
                linkIdx = linkData[linkIdx * LINK_SIZE];
            }
            linkIdx = nodeData[n * NODE_SIZE + 1];
            while (linkIdx >= 0) {
                iLen[n]++;
                linkIdx = linkData[linkIdx * LINK_SIZE + 1];
            }
        }

        // Pass 2: compute offsets (prefix sum)
        int outTotal = 0;
        int inTotal = 0;
        for (int n = 0; n < nodeCount; n++) {
            oOff[n] = outTotal;
            outTotal += oLen[n];
            iOff[n] = inTotal;
            inTotal += iLen[n];
        }

        // Pass 3: fill CSR edge arrays
        int[] oEdges = new int[outTotal * CSR_STRIDE];
        int[] iEdges = new int[inTotal * CSR_STRIDE];
        int[] oCursor = new int[nodeCount]; // temporary write cursors
        int[] iCursor = new int[nodeCount];
        System.arraycopy(oOff, 0, oCursor, 0, nodeCount);
        System.arraycopy(iOff, 0, iCursor, 0, nodeCount);

        for (int n = 0; n < nodeCount; n++) {
            // out-edges
            int linkIdx = nodeData[n * NODE_SIZE];
            while (linkIdx >= 0) {
                int slot = oCursor[n]++;
                int base = slot * CSR_STRIDE;
                int lBase = linkIdx * LINK_SIZE;
                oEdges[base + CSR_LINK_IDX]  = linkIdx;
                oEdges[base + CSR_TO_NODE]   = linkData[lBase + 3]; // toNode
                oEdges[base + CSR_FROM_NODE] = linkData[lBase + 2]; // fromNode
                oEdges[base + CSR_LENGTH]    = linkData[lBase + 4];
                oEdges[base + CSR_FREESPEED] = linkData[lBase + 5];
                linkIdx = linkData[lBase]; // next out-link
            }
            // in-edges
            linkIdx = nodeData[n * NODE_SIZE + 1];
            while (linkIdx >= 0) {
                int slot = iCursor[n]++;
                int base = slot * CSR_STRIDE;
                int lBase = linkIdx * LINK_SIZE;
                iEdges[base + CSR_LINK_IDX]  = linkIdx;
                iEdges[base + CSR_TO_NODE]   = linkData[lBase + 2]; // fromNode (=the "other" node for in-edges)
                iEdges[base + CSR_FROM_NODE] = linkData[lBase + 3]; // toNode
                iEdges[base + CSR_LENGTH]    = linkData[lBase + 4];
                iEdges[base + CSR_FREESPEED] = linkData[lBase + 5];
                linkIdx = linkData[lBase + 1]; // next in-link
            }
        }

        // Publish all arrays atomically via the synchronized method
        this.outOff = oOff;
        this.outLen = oLen;
        this.outEdges = oEdges;
        this.inOff = iOff;
        this.inLen = iLen;
        this.inEdges = iEdges;
    }

    /**
     * Returns whether CSR arrays have been built for this graph.
     */
    public boolean hasCSR() {
        return this.outOff != null;
    }

    public LinkIterator getOutLinkIterator() {
        if (outOff != null) {
            return new CSROutLinkIterator(this);
        }
        return new OutLinkIterator(this);
    }

    public LinkIterator getInLinkIterator() {
        if (inOff != null) {
            return new CSRInLinkIterator(this);
        }
        return new InLinkIterator(this);
    }

    Link getLink(int index) {
        return this.links[index];
    }

    Node getNode(int index) {
        return this.nodes[index];
    }

    Optional<TurnRestrictionsContext> getTurnRestrictions() {
			return Optional.ofNullable(this.turnRestrictions);
		}

    public interface LinkIterator {

        void reset(int nodeIdx);

        boolean next();

        int getLinkIndex();

        int getToNodeIndex();

        int getFromNodeIndex();

        double getLength();

        double getFreespeedTravelTime();
    }

    // -----------------------------------------------------------------------
    // Default linked-list iterators (original implementation)
    // -----------------------------------------------------------------------

    private static abstract class AbstractLinkIterator implements LinkIterator {

        final SpeedyGraph graph;
        int nodeIdx = -1;
        int linkIdx = -1;

        AbstractLinkIterator(SpeedyGraph graph) {
            this.graph = graph;
        }

        @Override
        final public void reset(int nodeIdx) {
            this.nodeIdx = nodeIdx;
            this.linkIdx = -1;
        }

        @Override
        abstract public boolean next();

        @Override
        final public int getLinkIndex() {
            return this.linkIdx;
        }

        @Override
        final public int getFromNodeIndex() {
            return this.graph.linkData[this.linkIdx * LINK_SIZE + 2];
        }

        @Override
        final public int getToNodeIndex() {
            return this.graph.linkData[this.linkIdx * LINK_SIZE + 3];
        }

        @Override
        final public double getLength() {
            return this.graph.linkData[this.linkIdx * LINK_SIZE + 4] / 100.0;
        }

        @Override
        final public double getFreespeedTravelTime() {
            return this.graph.linkData[this.linkIdx * LINK_SIZE + 5] / 100.0;
        }
    }

    private static class OutLinkIterator extends AbstractLinkIterator {

        OutLinkIterator(SpeedyGraph graph) {
            super(graph);
        }

        @Override
        public boolean next() {
            if (this.nodeIdx < 0) {
                return false;
            }
            if (this.linkIdx < 0) {
                this.linkIdx = graph.nodeData[this.nodeIdx * NODE_SIZE];
            } else {
                this.linkIdx = graph.linkData[this.linkIdx * LINK_SIZE];
            }
            if (this.linkIdx < 0) {
                this.nodeIdx = -1;
                return false;
            }
            return true;
        }
    }

    private static class InLinkIterator extends AbstractLinkIterator {

        InLinkIterator(SpeedyGraph graph) {
            super(graph);
        }

        @Override
        public boolean next() {
            if (this.nodeIdx < 0) {
                return false;
            }
            if (this.linkIdx < 0) {
                this.linkIdx = graph.nodeData[this.nodeIdx * NODE_SIZE + 1];
            } else {
                this.linkIdx = graph.linkData[this.linkIdx * LINK_SIZE + 1];
            }
            if (this.linkIdx < 0) {
                this.nodeIdx = -1;
                return false;
            }
            return true;
        }
    }

    // -----------------------------------------------------------------------
    // CSR-based iterators (cache-local, contiguous edge data per node)
    // -----------------------------------------------------------------------

    /**
     * CSR out-link iterator.  All out-edges for a node are stored contiguously
     * in {@link #outEdges} with colocated linkIdx, toNode, fromNode, length
     * and freespeed data — no pointer chasing required.
     */
    private static class CSROutLinkIterator implements LinkIterator {

        private final SpeedyGraph graph;
        private int cursor;
        private int end;
        private int base; // cursor * CSR_STRIDE (cached to avoid repeated multiply)

        CSROutLinkIterator(SpeedyGraph graph) {
            this.graph = graph;
        }

        @Override
        public void reset(int nodeIdx) {
            this.cursor = graph.outOff[nodeIdx];
            this.end = this.cursor + graph.outLen[nodeIdx];
            this.base = -1;
        }

        @Override
        public boolean next() {
            if (cursor >= end) {
                return false;
            }
            base = cursor * CSR_STRIDE;
            cursor++;
            return true;
        }

        @Override
        public int getLinkIndex() {
            return graph.outEdges[base + CSR_LINK_IDX];
        }

        @Override
        public int getToNodeIndex() {
            return graph.outEdges[base + CSR_TO_NODE];
        }

        @Override
        public int getFromNodeIndex() {
            return graph.outEdges[base + CSR_FROM_NODE];
        }

        @Override
        public double getLength() {
            return graph.outEdges[base + CSR_LENGTH] / 100.0;
        }

        @Override
        public double getFreespeedTravelTime() {
            return graph.outEdges[base + CSR_FREESPEED] / 100.0;
        }
    }

    /**
     * CSR in-link iterator.  All in-edges for a node are stored contiguously
     * in {@link #inEdges} with colocated linkIdx, fromNode, toNode, length
     * and freespeed data.
     */
    private static class CSRInLinkIterator implements LinkIterator {

        private final SpeedyGraph graph;
        private int cursor;
        private int end;
        private int base;

        CSRInLinkIterator(SpeedyGraph graph) {
            this.graph = graph;
        }

        @Override
        public void reset(int nodeIdx) {
            this.cursor = graph.inOff[nodeIdx];
            this.end = this.cursor + graph.inLen[nodeIdx];
            this.base = -1;
        }

        @Override
        public boolean next() {
            if (cursor >= end) {
                return false;
            }
            base = cursor * CSR_STRIDE;
            cursor++;
            return true;
        }

        @Override
        public int getLinkIndex() {
            return graph.inEdges[base + CSR_LINK_IDX];
        }

        @Override
        public int getToNodeIndex() {
            // For in-edges: "toNode" returns the fromNode of the link (the other end)
            return graph.inEdges[base + CSR_TO_NODE];
        }

        @Override
        public int getFromNodeIndex() {
            // For in-edges: "fromNode" returns the toNode of the link (the node we're iterating)
            return graph.inEdges[base + CSR_FROM_NODE];
        }

        @Override
        public double getLength() {
            return graph.inEdges[base + CSR_LENGTH] / 100.0;
        }

        @Override
        public double getFreespeedTravelTime() {
            return graph.inEdges[base + CSR_FREESPEED] / 100.0;
        }
    }
}
