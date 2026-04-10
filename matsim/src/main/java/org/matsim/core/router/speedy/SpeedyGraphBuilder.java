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

	/**
	 * Builds a routing graph with Z-order (Morton) spatial node reordering.
	 *
	 * <p>Spatially nearby nodes receive adjacent internal indices, which improves
	 * CPU cache locality during CH (Contraction Hierarchy) preprocessing.
	 * This method is intended <b>exclusively for CHRouter</b>.  All other routers
	 * should use {@link #build(Network, String)} which preserves the original
	 * identity node ordering and guarantees backward-compatible routing results.
	 *
	 * @param network the network to build the graph from
	 * @return a SpeedyGraph with Z-order spatial node ordering
	 */
	public static SpeedyGraph buildWithSpatialOrdering(Network network) {
		if (NetworkUtils.hasTurnRestrictions(network)) {
			return new SpeedyGraphBuilder().buildWithTurnRestrictionsSpatialOrder(network, null);
		}
		return new SpeedyGraphBuilder().buildWithoutTurnRestrictionsSpatialOrder(network);
	}

	private SpeedyGraph buildWithTurnRestrictions(Network network, @Nullable String turnRestrictionsMode) {

		TurnRestrictionsContext context = TurnRestrictionsContext.build(network, turnRestrictionsMode);

		// create routing graph from context
		this.nodeCount = context.getNodeCount();
		this.linkCount = context.getLinkCount();

		this.nodeData = new int[this.nodeCount * SpeedyGraph.NODE_SIZE];
		this.linkData = new int[this.linkCount * SpeedyGraph.LINK_SIZE];
		this.links = new Link[this.linkCount];
		this.nodes = new Node[this.nodeCount];

		Arrays.fill(this.nodeData, -1);
		Arrays.fill(this.linkData, -1);

		for (Node node : network.getNodes().values()) {
			this.nodes[node.getId().index()] = node;
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
			this.nodes[node.index()] = node.node();
		}
		for (TurnRestrictionsContext.ColoredLink link : context.coloredLinks) {
			addLink(link);
		}

		return new SpeedyGraph(this.nodeData, this.linkData, this.nodes, this.links, context);
	}

	private SpeedyGraph buildWithoutTurnRestrictions(Network network) {
		this.nodeCount = Id.getNumberOfIds(Node.class);
		this.linkCount = Id.getNumberOfIds(Link.class);

		this.nodeData = new int[this.nodeCount * SpeedyGraph.NODE_SIZE];
		this.linkData = new int[this.linkCount * SpeedyGraph.LINK_SIZE];
		this.links = new Link[this.linkCount];
		this.nodes = new Node[this.nodeCount];

		Arrays.fill(this.nodeData, -1);
		Arrays.fill(this.linkData, -1);

		for (Node node : network.getNodes().values()) {
			this.nodes[node.getId().index()] = node;
		}
		List<Id<Link>> linkIds = new ArrayList<>(network.getLinks().keySet());
        Collections.sort(linkIds);
        for (Id<Link> linkId : linkIds) {
        	Link link = network.getLinks().get(linkId);
			addLink(link);
		}

		return new SpeedyGraph(this.nodeData, this.linkData, this.nodes, this.links, null);
	}

	// -----------------------------------------------------------------------
	// Spatial ordering build methods (used exclusively by CHRouter)
	// -----------------------------------------------------------------------

	/** Maximum coordinate value for 16-bit Morton encoding (each axis uses 16 bits). */
	private static final int MORTON_COORD_MAX = 0xFFFF;

	private SpeedyGraph buildWithoutTurnRestrictionsSpatialOrder(Network network) {
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
			addLinkReordered(link);
		}

		return new SpeedyGraph(this.nodeData, this.linkData, this.nodes, this.links, null, this.nodeReorder);
	}

	private SpeedyGraph buildWithTurnRestrictionsSpatialOrder(Network network, @Nullable String turnRestrictionsMode) {

		TurnRestrictionsContext context = TurnRestrictionsContext.build(network, turnRestrictionsMode);

		int realNodeCount = network.getNodes().size();
		int coloredNodeCount = context.coloredNodes.size();
		int totalNodes = realNodeCount + coloredNodeCount;
		int totalSlots = context.getNodeCount();

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
			coords[idx] = cn.node().getCoord();
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
				addLinkReordered(link);
			}
		}
		for (TurnRestrictionsContext.ColoredNode node : context.coloredNodes) {
			this.nodes[this.nodeReorder[node.index()]] = node.node();
		}
		for (TurnRestrictionsContext.ColoredLink link : context.coloredLinks) {
			addLinkReordered(link);
		}

		return new SpeedyGraph(this.nodeData, this.linkData, this.nodes, this.links, context, this.nodeReorder);
	}

	// -----------------------------------------------------------------------
	// Link adding with spatial reordering
	// -----------------------------------------------------------------------

	private void addLinkReordered(Link link) {
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

	private void addLinkReordered(TurnRestrictionsContext.ColoredLink link) {
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

	private int resolveNodeIndex(Node node) {
		if (nodeReorder != null) {
			return nodeReorder[node.getId().index()];
		}
		return node.getId().index();
	}

	private int resolveExternalIndex(int externalIdx) {
		if (nodeReorder != null) {
			return nodeReorder[externalIdx];
		}
		return externalIdx;
	}

	// -----------------------------------------------------------------------
	// Spatial ordering helpers (Z-order / Morton curve)
	// -----------------------------------------------------------------------

	/**
	 * Computes a spatial ordering using the Z-order (Morton) curve.
	 */
	static int[] computeSpatialOrder(int[] externalIndices, Coord[] coords, int reorderSize) {
		int n = externalIndices.length;
		if (n == 0) {
			int[] reorder = new int[reorderSize];
			Arrays.fill(reorder, -1);
			return reorder;
		}

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
		if (rangeX < 1e-9) rangeX = 1.0;
		if (rangeY < 1e-9) rangeY = 1.0;

		long[] mortonKeys = new long[n];
		for (int i = 0; i < n; i++) {
			Coord c = coords[i];
			int nx = (int) (((c.getX() - minX) / rangeX) * MORTON_COORD_MAX);
			int ny = (int) (((c.getY() - minY) / rangeY) * MORTON_COORD_MAX);
			nx = Math.max(0, Math.min(MORTON_COORD_MAX, nx));
			ny = Math.max(0, Math.min(MORTON_COORD_MAX, ny));
			int morton = mortonEncode(nx, ny);
			mortonKeys[i] = ((long) (morton ^ 0x80000000) << 32) | (externalIndices[i] & 0xFFFFFFFFL);
		}

		Arrays.sort(mortonKeys);

		int[] reorder = new int[reorderSize];
		Arrays.fill(reorder, -1);
		for (int rank = 0; rank < n; rank++) {
			int extIdx = (int) (mortonKeys[rank] & 0xFFFFFFFFL);
			reorder[extIdx] = rank;
		}

		return reorder;
	}

	static int mortonEncode(int x, int y) {
		return expandBits(x) | (expandBits(y) << 1);
	}

	private static int expandBits(int v) {
		v = (v | (v << 16)) & 0x0000FFFF;
		v = (v | (v << 8))  & 0x00FF00FF;
		v = (v | (v << 4))  & 0x0F0F0F0F;
		v = (v | (v << 2))  & 0x33333333;
		v = (v | (v << 1))  & 0x55555555;
		return v;
	}

	// -----------------------------------------------------------------------
	// Original link adding (identity ordering, no reordering)
	// -----------------------------------------------------------------------

	private void addLink(Link link) {
		int fromNodeIdx = link.getFromNode().getId().index();
		int toNodeIdx = link.getToNode().getId().index();
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
			fromNodeIdx = link.fromColoredNode.index();
		}
		if (link.fromNode != null) {
			fromNodeIdx = link.fromNode.getId().index();
		}
		if (link.toColoredNode != null) {
			toNodeIdx = link.toColoredNode.index();
		}
		if (link.toNode != null) {
			toNodeIdx = link.toNode.getId().index();
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
