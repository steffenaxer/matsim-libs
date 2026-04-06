package org.matsim.core.router.speedy;

import jakarta.annotation.Nullable;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.turnRestrictions.DisallowedNextLinks;
import org.matsim.core.network.turnRestrictions.TurnRestrictionsContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Creates a {@link SpeedyGraph} for a provided {@link Network}.
 *
 * <p>Nodes are renumbered using a Z-order (Morton) curve so that spatially
 * nearby nodes receive adjacent internal indices.  This improves CPU cache
 * locality during routing.  The reordering is applied in both the normal
 * and turn-restriction paths; colored (virtual) nodes inherit the
 * coordinate of their parent real node and are placed nearby in the ordering.
 *
 * @author mrieser / Simunto
 */
public class SpeedyGraphBuilder {

	private int nodeCount;
	private int linkCount;
	private int[] nodeData;
	private int[] linkData;
	private Link[] links;
	private Node[] nodes;
	private int[] nodeReorder; // maps external Id.index() → internal index (null for identity)

	@Deprecated // use build-method with additional mode argument
	public static SpeedyGraph build(Network network) {
		return build(network, null);
	}

	/**
	 * Builds a routing graph from a network considering all links without caring
	 * about allowedModes but considering {@link DisallowedNextLinks} (aka turn
	 * restrictions) of the given {@code turnRestrictionsMode}.
	 *
	 * If turnRestrictionsMode == null, see
	 * {@link TurnRestrictionsContext#build(Network, String)} for how this is
	 * handled.
	 *
	 * @param network
	 * @param turnRestrictionsMode
	 * @return
	 */
	public static SpeedyGraph build(Network network, @Nullable String turnRestrictionsMode) {
		if (NetworkUtils.hasTurnRestrictions(network)) {
			return new SpeedyGraphBuilder().buildWithTurnRestrictions(network, turnRestrictionsMode);
		}
		return new SpeedyGraphBuilder().buildWithoutTurnRestrictions(network);
	}

	private SpeedyGraph buildWithTurnRestrictions(Network network, @Nullable String turnRestrictionsMode) {

		TurnRestrictionsContext context = TurnRestrictionsContext.build(network, turnRestrictionsMode);

		// Collect all nodes (real + colored) with their external indices and coordinates
		// Colored nodes inherit the coordinate of their parent real node.
		int realNodeCount = network.getNodes().size();
		int coloredNodeCount = context.coloredNodes.size();
		int totalNodes = realNodeCount + coloredNodeCount;
		int totalSlots = context.getNodeCount(); // address space covering all external indices

		int[] extIndices = new int[totalNodes];
		Coord[] coords = new Coord[totalNodes];
		int idx = 0;
		for (Node node : network.getNodes().values()) {
			extIndices[idx] = node.getId().index();
			coords[idx] = node.getCoord();
			idx++;
		}
		for (TurnRestrictionsContext.ColoredNode cn : context.coloredNodes) {
			extIndices[idx] = cn.index();
			coords[idx] = cn.node().getCoord(); // same coord as parent real node
			idx++;
		}

		this.nodeReorder = computeSpatialOrder(extIndices, coords, totalSlots);
		this.nodeCount = totalNodes;
		this.linkCount = context.getLinkCount();

		this.nodeData = new int[this.nodeCount * SpeedyGraph.NODE_SIZE];
		this.linkData = new int[this.linkCount * SpeedyGraph.LINK_SIZE];
		this.links = new Link[this.linkCount];
		this.nodes = new Node[this.nodeCount];

		Arrays.fill(this.nodeData, -1);
		Arrays.fill(this.linkData, -1);

		for (Node node : network.getNodes().values()) {
			this.nodes[this.nodeReorder[node.getId().index()]] = node;
		}
		List<Id<Link>> linkIds = new ArrayList<>(network.getLinks().keySet());
        Collections.sort(linkIds);
        for (Id<Link> linkId : linkIds) {
        	Link link = network.getLinks().get(linkId);
			if (context.replacedLinks.get(link.getId()) == null) {
				addLink(link);
			}
		}
		for (TurnRestrictionsContext.ColoredNode node : context.coloredNodes) {
			this.nodes[this.nodeReorder[node.index()]] = node.node();
		}
		for (TurnRestrictionsContext.ColoredLink link : context.coloredLinks) {
			addLink(link);
		}

		return new SpeedyGraph(this.nodeData, this.linkData, this.nodes, this.links, context, this.nodeReorder);
	}

	private SpeedyGraph buildWithoutTurnRestrictions(Network network) {
		// Collect all nodes with their external indices and coordinates
		List<Node> networkNodes = new ArrayList<>(network.getNodes().values());
		int actualNodeCount = networkNodes.size();
		int totalIdSlots = Id.getNumberOfIds(Node.class);

		int[] extIndices = new int[actualNodeCount];
		Coord[] coords = new Coord[actualNodeCount];
		for (int i = 0; i < actualNodeCount; i++) {
			Node node = networkNodes.get(i);
			extIndices[i] = node.getId().index();
			coords[i] = node.getCoord();
		}

		// Build the Z-order mapping: external Id.index() → dense internal index
		this.nodeReorder = computeSpatialOrder(extIndices, coords, totalIdSlots);
		this.nodeCount = actualNodeCount;
		this.linkCount = Id.getNumberOfIds(Link.class);

		this.nodeData = new int[this.nodeCount * SpeedyGraph.NODE_SIZE];
		this.linkData = new int[this.linkCount * SpeedyGraph.LINK_SIZE];
		this.links = new Link[this.linkCount];
		this.nodes = new Node[this.nodeCount];

		Arrays.fill(this.nodeData, -1);
		Arrays.fill(this.linkData, -1);

		for (Node node : networkNodes) {
			int internalIdx = this.nodeReorder[node.getId().index()];
			this.nodes[internalIdx] = node;
		}
		List<Id<Link>> linkIds = new ArrayList<>(network.getLinks().keySet());
        Collections.sort(linkIds);
        for (Id<Link> linkId : linkIds) {
        	Link link = network.getLinks().get(linkId);
			addLink(link);
		}

		return new SpeedyGraph(this.nodeData, this.linkData, this.nodes, this.links, null, this.nodeReorder);
	}

	/** Maximum coordinate value for 16-bit Morton encoding (each axis uses 16 bits). */
	private static final int MORTON_COORD_MAX = 0xFFFF;

	/**
	 * Builds a SpeedyGraph <b>without</b> Z-order spatial node reordering.
	 * Nodes receive dense internal indices in the natural order of their external IDs.
	 * This is package-private and intended only for benchmarking comparisons.
	 */
	static SpeedyGraph buildWithIdentityOrdering(Network network) {
		return new SpeedyGraphBuilder().buildWithoutTurnRestrictionsIdentity(network);
	}

	private SpeedyGraph buildWithoutTurnRestrictionsIdentity(Network network) {
		List<Node> networkNodes = new ArrayList<>(network.getNodes().values());
		int actualNodeCount = networkNodes.size();
		int totalIdSlots = Id.getNumberOfIds(Node.class);

		int[] extIndices = new int[actualNodeCount];
		for (int i = 0; i < actualNodeCount; i++) {
			extIndices[i] = networkNodes.get(i).getId().index();
		}

		this.nodeReorder = computeIdentityOrder(extIndices, totalIdSlots);
		this.nodeCount = actualNodeCount;
		this.linkCount = Id.getNumberOfIds(Link.class);

		this.nodeData = new int[this.nodeCount * SpeedyGraph.NODE_SIZE];
		this.linkData = new int[this.linkCount * SpeedyGraph.LINK_SIZE];
		this.links = new Link[this.linkCount];
		this.nodes = new Node[this.nodeCount];

		Arrays.fill(this.nodeData, -1);
		Arrays.fill(this.linkData, -1);

		for (Node node : networkNodes) {
			int internalIdx = this.nodeReorder[node.getId().index()];
			this.nodes[internalIdx] = node;
		}
		List<Id<Link>> linkIds = new ArrayList<>(network.getLinks().keySet());
		Collections.sort(linkIds);
		for (Id<Link> linkId : linkIds) {
			Link link = network.getLinks().get(linkId);
			addLink(link);
		}

		return new SpeedyGraph(this.nodeData, this.linkData, this.nodes, this.links, null, this.nodeReorder);
	}

	/**
	 * Computes a dense identity ordering (no spatial reordering).
	 * Nodes are numbered by their external index order for a deterministic, non-spatial mapping.
	 */
	static int[] computeIdentityOrder(int[] externalIndices, int reorderSize) {
		int n = externalIndices.length;
		int[] sorted = externalIndices.clone();
		Arrays.sort(sorted);
		int[] reorder = new int[reorderSize];
		Arrays.fill(reorder, -1);
		for (int rank = 0; rank < n; rank++) {
			reorder[sorted[rank]] = rank;
		}
		return reorder;
	}

	/**
	 * Computes a spatial ordering using the Z-order (Morton) curve.
	 *
	 * @param externalIndices  external node indices (parallel to coords)
	 * @param coords           coordinates for each node (parallel to externalIndices)
	 * @param reorderSize      size of the returned mapping array (must be &gt; max(externalIndices))
	 * @return mapping: result[externalIdx] → dense rank (0..n-1), or -1 for unmapped slots
	 */
	static int[] computeSpatialOrder(int[] externalIndices, Coord[] coords, int reorderSize) {
		int n = externalIndices.length;
		if (n == 0) {
			int[] reorder = new int[reorderSize];
			Arrays.fill(reorder, -1);
			return reorder;
		}

		// Step 1: Compute bounding box
		double minX = Double.MAX_VALUE, maxX = -Double.MAX_VALUE;
		double minY = Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
		for (Coord c : coords) {
			if (c.getX() < minX) minX = c.getX();
			if (c.getX() > maxX) maxX = c.getX();
			if (c.getY() < minY) minY = c.getY();
			if (c.getY() > maxY) maxY = c.getY();
		}

		double rangeX = maxX - minX;
		double rangeY = maxY - minY;
		// Avoid division by zero for degenerate networks
		if (rangeX < 1e-9) rangeX = 1.0;
		if (rangeY < 1e-9) rangeY = 1.0;

		// Step 2: Compute Morton index for each node
		// Pack (mortonIndex, originalArrayPosition) into a long for sorting
		long[] mortonKeys = new long[n];
		for (int i = 0; i < n; i++) {
			Coord c = coords[i];
			int nx = (int) (((c.getX() - minX) / rangeX) * MORTON_COORD_MAX);
			int ny = (int) (((c.getY() - minY) / rangeY) * MORTON_COORD_MAX);
			nx = Math.max(0, Math.min(MORTON_COORD_MAX, nx));
			ny = Math.max(0, Math.min(MORTON_COORD_MAX, ny));
			int morton = mortonEncode(nx, ny);
			mortonKeys[i] = ((long) morton << 32) | (i & 0xFFFFFFFFL);
		}

		// Step 3: Sort by Morton index
		Arrays.sort(mortonKeys);

		// Step 4: Build mapping: externalIdx → dense sorted rank
		int[] reorder = new int[reorderSize];
		Arrays.fill(reorder, -1);
		for (int rank = 0; rank < n; rank++) {
			int origPos = (int) (mortonKeys[rank] & 0xFFFFFFFFL);
			reorder[externalIndices[origPos]] = rank;
		}

		return reorder;
	}

	/**
	 * Encodes two 16-bit coordinates into a 32-bit Morton (Z-order) code
	 * by interleaving their bits.
	 */
	static int mortonEncode(int x, int y) {
		return expandBits(x) | (expandBits(y) << 1);
	}

	/**
	 * Spreads the lower 16 bits of v into even bit positions of a 32-bit int.
	 * Uses a standard bit-interleave sequence: each step doubles the spacing
	 * between active bits by shifting and masking with progressively finer masks.
	 */
	private static int expandBits(int v) {
		v = (v | (v << 16)) & 0x0000FFFF; // keep lower 16 bits
		v = (v | (v << 8))  & 0x00FF00FF; // spread into 8-bit groups
		v = (v | (v << 4))  & 0x0F0F0F0F; // spread into 4-bit groups
		v = (v | (v << 2))  & 0x33333333;  // spread into 2-bit groups
		v = (v | (v << 1))  & 0x55555555;  // spread into single bits (even positions)
		return v;
	}

	private void addLink(Link link) {
		int fromNodeIdx = resolveNodeIndex(link.getFromNode());
		int toNodeIdx = resolveNodeIndex(link.getToNode());
		int linkIdx = link.getId().index();

		int base = linkIdx * SpeedyGraph.LINK_SIZE;
		this.linkData[base + 2] = fromNodeIdx;
		this.linkData[base + 3] = toNodeIdx;
		this.linkData[base + 4] = (int) Math.round(link.getLength() * 100.0);
		this.linkData[base + 5] = (int) Math.round(link.getLength() / link.getFreespeed() * 100.0);

		setOutLink(fromNodeIdx, linkIdx);
		setInLink(toNodeIdx, linkIdx);

		this.links[linkIdx] = link;
	}

	private void addLink(TurnRestrictionsContext.ColoredLink link) {
		int fromNodeIdx = -1;
		int toNodeIdx = -1;
		int linkIdx = link.index;

		if (link.fromColoredNode != null) {
			fromNodeIdx = resolveExternalIndex(link.fromColoredNode.index());
		}
		if (link.fromNode != null) {
			fromNodeIdx = resolveNodeIndex(link.fromNode);
		}
		if (link.toColoredNode != null) {
			toNodeIdx = resolveExternalIndex(link.toColoredNode.index());
		}
		if (link.toNode != null) {
			toNodeIdx = resolveNodeIndex(link.toNode);
		}

		int base = linkIdx * SpeedyGraph.LINK_SIZE;
		this.linkData[base + 2] = fromNodeIdx;
		this.linkData[base + 3] = toNodeIdx;
		this.linkData[base + 4] = (int) Math.round(link.link.getLength() * 100.0);
		this.linkData[base + 5] = (int) Math.round(link.link.getLength() / link.link.getFreespeed() * 100.0);

		setOutLink(fromNodeIdx, linkIdx);
		setInLink(toNodeIdx, linkIdx);

		this.links[linkIdx] = link.link;
	}

	/**
	 * Resolves a node to its internal index, applying spatial reordering if enabled.
	 */
	private int resolveNodeIndex(Node node) {
		if (nodeReorder != null) {
			return nodeReorder[node.getId().index()];
		}
		return node.getId().index();
	}

	/**
	 * Resolves a raw external index (e.g. from a colored node) to its internal index.
	 */
	private int resolveExternalIndex(int externalIdx) {
		if (nodeReorder != null) {
			return nodeReorder[externalIdx];
		}
		return externalIdx;
	}

	private void setOutLink(int fromNodeIdx, int linkIdx) {
		final int nodeI = fromNodeIdx * SpeedyGraph.NODE_SIZE;
		int outLinkIdx = this.nodeData[nodeI];
		if (outLinkIdx < 0) {
			this.nodeData[nodeI] = linkIdx;
			return;
		}
		int lastLinkIdx;
		do {
			lastLinkIdx = outLinkIdx;
			outLinkIdx = this.linkData[lastLinkIdx * SpeedyGraph.LINK_SIZE];
		} while (outLinkIdx >= 0);
		this.linkData[lastLinkIdx * SpeedyGraph.LINK_SIZE] = linkIdx;
	}

	private void setInLink(int toNodeIdx, int linkIdx) {
		final int nodeI = toNodeIdx * SpeedyGraph.NODE_SIZE + 1;
		int inLinkIdx = this.nodeData[nodeI];
		if (inLinkIdx < 0) {
			this.nodeData[nodeI] = linkIdx;
			return;
		}
		int lastLinkIdx;
		do {
			lastLinkIdx = inLinkIdx;
			inLinkIdx = this.linkData[lastLinkIdx * SpeedyGraph.LINK_SIZE + 1];
		} while (inLinkIdx >= 0);
		this.linkData[lastLinkIdx * SpeedyGraph.LINK_SIZE + 1] = linkIdx;
	}
}
