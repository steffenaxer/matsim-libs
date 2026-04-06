package org.matsim.core.router.speedy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.NetworkFactory;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.speedy.SpeedyGraph.LinkIterator;
import org.matsim.core.router.util.LeastCostPathCalculator.Path;

import java.util.*;

/**
 * Tests for the CSR (Compressed Sparse Row) optimization of {@link SpeedyGraph}.
 * Verifies that CSR-based iterators produce the same results as the default
 * linked-list iterators and benchmarks the performance difference.
 */
public class SpeedyGraphCSRTest {

	@Test
	void testCSROutLinksMatchLinkedList() {
		Id.resetCaches();
		Network network = createTestNetwork();
		SpeedyGraph graph = SpeedyGraphBuilder.build(network);

		// Collect edges using linked-list iterators
		Map<Integer, List<int[]>> linkedListOut = collectOutEdges(graph);

		// Build CSR and collect edges using CSR iterators
		graph.buildCSR();
		Assertions.assertTrue(graph.hasCSR());
		Map<Integer, List<int[]>> csrOut = collectOutEdges(graph);

		// Compare: same nodes, same edges (order may differ, so compare as sets)
		Assertions.assertEquals(linkedListOut.keySet(), csrOut.keySet());
		for (int node : linkedListOut.keySet()) {
			Set<Integer> llLinks = new HashSet<>();
			for (int[] edge : linkedListOut.get(node)) llLinks.add(edge[0]);
			Set<Integer> csrLinks = new HashSet<>();
			for (int[] edge : csrOut.get(node)) csrLinks.add(edge[0]);
			Assertions.assertEquals(llLinks, csrLinks,
					"Out-links mismatch for node " + node);
		}
	}

	@Test
	void testCSRInLinksMatchLinkedList() {
		Id.resetCaches();
		Network network = createTestNetwork();
		SpeedyGraph graph = SpeedyGraphBuilder.build(network);

		Map<Integer, List<int[]>> linkedListIn = collectInEdges(graph);

		graph.buildCSR();
		Map<Integer, List<int[]>> csrIn = collectInEdges(graph);

		Assertions.assertEquals(linkedListIn.keySet(), csrIn.keySet());
		for (int node : linkedListIn.keySet()) {
			Set<Integer> llLinks = new HashSet<>();
			for (int[] edge : linkedListIn.get(node)) llLinks.add(edge[0]);
			Set<Integer> csrLinks = new HashSet<>();
			for (int[] edge : csrIn.get(node)) csrLinks.add(edge[0]);
			Assertions.assertEquals(llLinks, csrLinks,
					"In-links mismatch for node " + node);
		}
	}

	@Test
	void testCSREdgeDataCorrectness() {
		Id.resetCaches();
		Network network = createTestNetwork();
		SpeedyGraph graph = SpeedyGraphBuilder.build(network);

		// Collect detailed edge data with linked-list
		Map<Integer, double[]> linkedListData = collectOutEdgeData(graph);

		graph.buildCSR();
		Map<Integer, double[]> csrData = collectOutEdgeData(graph);

		for (int linkIdx : linkedListData.keySet()) {
			Assertions.assertTrue(csrData.containsKey(linkIdx),
					"CSR missing link " + linkIdx);
			double[] ll = linkedListData.get(linkIdx);
			double[] csr = csrData.get(linkIdx);
			Assertions.assertEquals(ll[0], csr[0], "fromNode mismatch for link " + linkIdx);
			Assertions.assertEquals(ll[1], csr[1], "toNode mismatch for link " + linkIdx);
			Assertions.assertEquals(ll[2], csr[2], 1e-2, "length mismatch for link " + linkIdx);
			Assertions.assertEquals(ll[3], csr[3], 1e-2, "freespeedTT mismatch for link " + linkIdx);
		}
	}

	@Test
	void testCSRIdempotent() {
		Id.resetCaches();
		Network network = createTestNetwork();
		SpeedyGraph graph = SpeedyGraphBuilder.build(network);

		graph.buildCSR();
		Assertions.assertTrue(graph.hasCSR());

		// Second call should be a no-op
		graph.buildCSR();
		Assertions.assertTrue(graph.hasCSR());
	}

	@Test
	void testCSRDijkstraCorrectness() {
		Id.resetCaches();
		Network network = createTestNetwork();
		FreespeedTravelTimeAndDisutility tc =
				new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

		// Run Dijkstra without CSR
		SpeedyGraph graph = SpeedyGraphBuilder.build(network);
		SpeedyDijkstra dijkstraLL = new SpeedyDijkstra(graph, tc, tc);
		List<Node> nodeList = new ArrayList<>(network.getNodes().values());
		Map<String, Double> llCosts = new HashMap<>();
		for (Node from : nodeList) {
			for (Node to : nodeList) {
				if (from == to) continue;
				Path path = dijkstraLL.calcLeastCostPath(from, to, 8 * 3600, null, null);
				if (path != null) {
					llCosts.put(from.getId() + "->" + to.getId(), path.travelCost);
				}
			}
		}

		// Run Dijkstra with CSR
		SpeedyGraph graph2 = SpeedyGraphBuilder.build(network);
		graph2.buildCSR();
		SpeedyDijkstra dijkstraCSR = new SpeedyDijkstra(graph2, tc, tc);
		for (Node from : nodeList) {
			for (Node to : nodeList) {
				if (from == to) continue;
				Path path = dijkstraCSR.calcLeastCostPath(from, to, 8 * 3600, null, null);
				String key = from.getId() + "->" + to.getId();
				if (path != null) {
					Assertions.assertTrue(llCosts.containsKey(key),
							"CSR found path " + key + " but linked-list didn't");
					Assertions.assertEquals(llCosts.get(key), path.travelCost, 1e-6,
							"Cost mismatch for " + key);
				} else {
					Assertions.assertFalse(llCosts.containsKey(key),
							"CSR didn't find path " + key + " but linked-list did");
				}
			}
		}
	}

	@Test
	void benchmarkCSRvsLinkedList() {
		// Generate a larger random grid network for benchmarking
		Id.resetCaches();
		int gridSize = 100; // 100×100 = 10,000 nodes, ~40,000 links
		Network network = createGridNetwork(gridSize);
		FreespeedTravelTimeAndDisutility tc =
				new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

		// Build graph without CSR
		SpeedyGraph graphLL = SpeedyGraphBuilder.build(network);
		// Build graph with CSR
		SpeedyGraph graphCSR = SpeedyGraphBuilder.build(network);
		graphCSR.buildCSR();

		List<Node> nodeList = new ArrayList<>(network.getNodes().values());
		int n = nodeList.size();
		Random rng = new Random(42);

		int numQueries = 2000;
		int warmup = 100;

		// Prepare random pairs
		int[][] pairs = new int[numQueries + warmup][2];
		for (int i = 0; i < pairs.length; i++) {
			pairs[i][0] = rng.nextInt(n);
			pairs[i][1] = rng.nextInt(n);
		}

		// -- Benchmark Linked-List --
		SpeedyDijkstra dijkstraLL = new SpeedyDijkstra(graphLL, tc, tc);
		for (int i = 0; i < warmup; i++) {
			dijkstraLL.calcLeastCostPath(nodeList.get(pairs[i][0]), nodeList.get(pairs[i][1]),
					8 * 3600, null, null);
		}
		long llStart = System.nanoTime();
		for (int i = warmup; i < warmup + numQueries; i++) {
			dijkstraLL.calcLeastCostPath(nodeList.get(pairs[i][0]), nodeList.get(pairs[i][1]),
					8 * 3600, null, null);
		}
		long llNs = System.nanoTime() - llStart;

		// -- Benchmark CSR --
		SpeedyDijkstra dijkstraCSR = new SpeedyDijkstra(graphCSR, tc, tc);
		for (int i = 0; i < warmup; i++) {
			dijkstraCSR.calcLeastCostPath(nodeList.get(pairs[i][0]), nodeList.get(pairs[i][1]),
					8 * 3600, null, null);
		}
		long csrStart = System.nanoTime();
		for (int i = warmup; i < warmup + numQueries; i++) {
			dijkstraCSR.calcLeastCostPath(nodeList.get(pairs[i][0]), nodeList.get(pairs[i][1]),
					8 * 3600, null, null);
		}
		long csrNs = System.nanoTime() - csrStart;

		double llAvgUs  = llNs  / (numQueries * 1000.0);
		double csrAvgUs = csrNs / (numQueries * 1000.0);
		double speedup  = (double) llNs / Math.max(1, csrNs);

		System.out.println();
		System.out.println("═══════════════════════════════════════════════════════════");
		System.out.println("  SpeedyGraph CSR Benchmark  (" + gridSize + "×" + gridSize
				+ " grid, " + network.getNodes().size() + " nodes, "
				+ network.getLinks().size() + " links)");
		System.out.println("═══════════════════════════════════════════════════════════");
		System.out.printf("  Queries:        %,d  (warmup: %d)%n", numQueries, warmup);
		System.out.printf("  Linked-List:    %,.1f µs/query%n", llAvgUs);
		System.out.printf("  CSR:            %,.1f µs/query%n", csrAvgUs);
		System.out.printf("  Speedup:        %.2fx%n", speedup);
		System.out.println("═══════════════════════════════════════════════════════════");

		// Verify correctness on a few random queries
		rng = new Random(999);
		for (int i = 0; i < 50; i++) {
			Node from = nodeList.get(rng.nextInt(n));
			Node to = nodeList.get(rng.nextInt(n));
			Path pathLL = dijkstraLL.calcLeastCostPath(from, to, 8 * 3600, null, null);
			Path pathCSR = dijkstraCSR.calcLeastCostPath(from, to, 8 * 3600, null, null);
			if (pathLL == null) {
				Assertions.assertNull(pathCSR);
			} else {
				Assertions.assertNotNull(pathCSR);
				Assertions.assertEquals(pathLL.travelCost, pathCSR.travelCost, 1e-6,
						"Cost mismatch: " + from.getId() + " -> " + to.getId());
			}
		}
	}

	// ---- Helpers ----

	private Map<Integer, List<int[]>> collectOutEdges(SpeedyGraph graph) {
		Map<Integer, List<int[]>> result = new HashMap<>();
		LinkIterator li = graph.getOutLinkIterator();
		for (int n = 0; n < graph.nodeCount; n++) {
			if (graph.getNode(n) == null) continue;
			li.reset(n);
			List<int[]> edges = new ArrayList<>();
			while (li.next()) {
				edges.add(new int[] { li.getLinkIndex(), li.getToNodeIndex(), li.getFromNodeIndex() });
			}
			if (!edges.isEmpty()) {
				result.put(n, edges);
			}
		}
		return result;
	}

	private Map<Integer, List<int[]>> collectInEdges(SpeedyGraph graph) {
		Map<Integer, List<int[]>> result = new HashMap<>();
		LinkIterator li = graph.getInLinkIterator();
		for (int n = 0; n < graph.nodeCount; n++) {
			if (graph.getNode(n) == null) continue;
			li.reset(n);
			List<int[]> edges = new ArrayList<>();
			while (li.next()) {
				edges.add(new int[] { li.getLinkIndex(), li.getToNodeIndex(), li.getFromNodeIndex() });
			}
			if (!edges.isEmpty()) {
				result.put(n, edges);
			}
		}
		return result;
	}

	private Map<Integer, double[]> collectOutEdgeData(SpeedyGraph graph) {
		Map<Integer, double[]> result = new HashMap<>();
		LinkIterator li = graph.getOutLinkIterator();
		for (int n = 0; n < graph.nodeCount; n++) {
			if (graph.getNode(n) == null) continue;
			li.reset(n);
			while (li.next()) {
				result.put(li.getLinkIndex(), new double[] {
						li.getFromNodeIndex(), li.getToNodeIndex(),
						li.getLength(), li.getFreespeedTravelTime()
				});
			}
		}
		return result;
	}

	private Network createTestNetwork() {
		Network network = NetworkUtils.createNetwork();
		NetworkFactory nf = network.getFactory();

		Node n1 = nf.createNode(Id.create("1", Node.class), new Coord(0, 1000));
		Node n2 = nf.createNode(Id.create("2", Node.class), new Coord(5000, 1000));
		Node n3 = nf.createNode(Id.create("3", Node.class), new Coord(0, 300));
		Node n4 = nf.createNode(Id.create("4", Node.class), new Coord(2000, 300));
		Node n5 = nf.createNode(Id.create("5", Node.class), new Coord(0, 0));
		Node n6 = nf.createNode(Id.create("6", Node.class), new Coord(5000, 0));

		network.addNode(n1); network.addNode(n2); network.addNode(n3);
		network.addNode(n4); network.addNode(n5); network.addNode(n6);

		addLink(network, nf, "12", n1, n2, 5000, 80 / 3.6);
		addLink(network, nf, "21", n2, n1, 5000, 80 / 3.6);
		addLink(network, nf, "13", n1, n3, 900, 60 / 3.6);
		addLink(network, nf, "14", n1, n4, 3500, 50 / 3.6);
		addLink(network, nf, "34", n3, n4, 2500, 50 / 3.6);
		addLink(network, nf, "35", n3, n5, 300, 60 / 3.6);
		addLink(network, nf, "46", n4, n6, 3000, 40 / 3.6);
		addLink(network, nf, "56", n5, n6, 5200, 80 / 3.6);
		addLink(network, nf, "65", n6, n5, 5200, 80 / 3.6);
		addLink(network, nf, "62", n6, n2, 1200, 60 / 3.6);

		return network;
	}

	private Network createGridNetwork(int gridSize) {
		Network network = NetworkUtils.createNetwork();
		NetworkFactory nf = network.getFactory();

		// Create gridSize × gridSize nodes
		Node[][] nodes = new Node[gridSize][gridSize];
		for (int x = 0; x < gridSize; x++) {
			for (int y = 0; y < gridSize; y++) {
				String id = x + "_" + y;
				nodes[x][y] = nf.createNode(Id.create(id, Node.class),
						new Coord(x * 1000.0, y * 1000.0));
				network.addNode(nodes[x][y]);
			}
		}

		// Create links: right, left, up, down
		for (int x = 0; x < gridSize; x++) {
			for (int y = 0; y < gridSize; y++) {
				if (x < gridSize - 1) {
					addLink(network, nf, x + "_" + y + "_R", nodes[x][y], nodes[x + 1][y], 1000, 50 / 3.6);
					addLink(network, nf, x + "_" + y + "_L", nodes[x + 1][y], nodes[x][y], 1000, 50 / 3.6);
				}
				if (y < gridSize - 1) {
					addLink(network, nf, x + "_" + y + "_U", nodes[x][y], nodes[x][y + 1], 1000, 50 / 3.6);
					addLink(network, nf, x + "_" + y + "_D", nodes[x][y + 1], nodes[x][y], 1000, 50 / 3.6);
				}
			}
		}

		return network;
	}

	private void addLink(Network network, NetworkFactory nf, String id,
						 Node from, Node to, double length, double freespeed) {
		Link link = nf.createLink(Id.create(id, Link.class), from, to);
		link.setLength(length);
		link.setFreespeed(freespeed);
		link.setCapacity(2000);
		network.addLink(link);
	}
}
