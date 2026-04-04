package org.matsim.core.router.speedy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.NetworkFactory;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator.Path;
import org.matsim.core.scenario.ScenarioUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Large-scale tests and benchmarks for nested-dissection-ordered CH contraction
 * ({@link InertialFlowCutter} + {@link SpeedyCHBuilder#buildWithOrder}).
 *
 * <p>Tests on real and large synthetic networks to evaluate performance and correctness
 * at realistic scales:
 * <ul>
 *   <li><b>Real networks</b>: Berlin (11.5k nodes)</li>
 *   <li><b>Synthetic large grids</b>: up to 40,000 nodes with perturbed coordinates</li>
 *   <li><b>Berlin v7.0</b>: ~65k nodes, loaded from URL when accessible</li>
 * </ul>
 */
public class SpeedyCHBuilderLargeNetworkTest {

    private static final int    NUM_QUERIES    = 500;
    private static final double COST_TOLERANCE = 1e-6;

    // -----------------------------------------------------------------------
    // Correctness tests on real natural road networks
    // -----------------------------------------------------------------------

    /**
     * Berlin road network: 11,566 nodes, 27,664 links.
     * Same network used in {@link SpeedyCHBuilderNDTest} but tested here
     * as part of the large-network suite.
     */
    @Test
    void testNDCorrectnessBerlinNetwork() {
        Network network = loadNetwork("test/scenarios/berlin/network.xml.gz");
        System.out.printf("%nBerlin network: %,d nodes, %,d links%n",
                network.getNodes().size(), network.getLinks().size());
        runCorrectnessTest(network);
    }

    // -----------------------------------------------------------------------
    // Large synthetic network tests
    // -----------------------------------------------------------------------

    /**
     * 100×100 perturbed grid (10,000 nodes, ~39,600 links).
     * Coordinates are randomly shifted to break regularity – closer to a real road network.
     */
    @Test
    void testNDCorrectnessLargeGrid100x100() {
        Network network = buildPerturbedGrid(100, 42);
        System.out.printf("%n100x100 perturbed grid: %,d nodes, %,d links%n",
                network.getNodes().size(), network.getLinks().size());
        runCorrectnessTest(network);
    }

    /**
     * 150×150 perturbed grid (22,500 nodes, ~89,400 links) — large synthetic network.
     * Tests correctness with 500 random OD pairs.
     */
    @Test
    void testNDCorrectnessLargeGrid150x150() {
        Network network = buildPerturbedGrid(150, 42);
        System.out.printf("%n150x150 perturbed grid: %,d nodes, %,d links%n",
                network.getNodes().size(), network.getLinks().size());
        runCorrectnessTest(network);
    }

    // -----------------------------------------------------------------------
    // Multi-network benchmark
    // -----------------------------------------------------------------------

    /**
     * Comprehensive benchmark comparing ND vs witness-based CH ordering across
     * all available real and synthetic networks.
     *
     * <p>Reports build time, edge count, shortcut overhead, and query throughput.
     */
    @Test
    void benchmarkAllNetworks() {
        System.out.printf("%n%n╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗%n");
        System.out.printf(    "║                            CH Build & Query Benchmark – ND vs Witness-based                                     ║%n");
        System.out.printf(    "╠════════════════════════╤═══════╤═══════╤═══════════╤═══════════╤══════════╤══════════╤═══════════╤════════════════╣%n");
        System.out.printf(    "║ Network                │ Nodes │ Links │ Witness   │ ND total  │  ND ord  │ ND contr │ Edge ovhd │ Query speedup ║%n");
        System.out.printf(    "║                        │       │       │ build(ms) │ build(ms) │  (ms)    │   (ms)   │   (%%)     │ ND/Dijkstra   ║%n");
        System.out.printf(    "╠════════════════════════╪═══════╪═══════╪═══════════╪═══════════╪══════════╪══════════╪═══════════╪════════════════╣%n");

        // Berlin (existing test network ~11.5k)
        benchmarkNetwork("Berlin-11k",
                loadNetwork("test/scenarios/berlin/network.xml.gz"));

        // Large synthetic grids
        benchmarkNetwork("Grid-50x50",  buildPerturbedGrid(50, 42));
        benchmarkNetwork("Grid-100x100", buildPerturbedGrid(100, 42));
        benchmarkNetwork("Grid-150x150", buildPerturbedGrid(150, 42));

        System.out.printf("╚════════════════════════╧═══════╧═══════╧═══════════╧═══════════╧══════════╧══════════╧═══════════╧════════════════╝%n%n");
    }

    // -----------------------------------------------------------------------
    // Berlin v7.0 download test (when accessible)
    // -----------------------------------------------------------------------

    /**
     * Tests the Berlin v7.0 network (~65k nodes) downloaded from the TU Berlin SVN server.
     *
     * <p>This test downloads the network from:
     * {@code https://svn.vsp.tu-berlin.de/repos/public-svn/matsim/scenarios/countries/de/berlin/berlin-v7.0/input/berlin-v7.0-network.xml.gz}
     *
     * <p>It is skipped if the file cannot be downloaded (e.g. network restrictions).
     */
    @Test
    void testNDCorrectnessBerlinV70Network() {
        String url = "https://svn.vsp.tu-berlin.de/repos/public-svn/matsim/scenarios/countries/de/berlin/berlin-v7.0/input/berlin-v7.0-network.xml.gz";
        String localPath = "/tmp/berlin-v7.0-network.xml.gz";

        // Try to download if not cached
        File f = new File(localPath);
        if (!f.exists() || f.length() < 1000) {
            try {
                var pb = new ProcessBuilder("curl", "-sL", "-o", localPath, url);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                int exit = p.waitFor();
                if (exit != 0 || !f.exists() || f.length() < 1000) {
                    System.out.println("SKIPPED: Berlin v7.0 network not accessible (download failed).");
                    return;
                }
            } catch (Exception e) {
                System.out.println("SKIPPED: Berlin v7.0 network not accessible: " + e.getMessage());
                return;
            }
        }

        // Load and test
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        try {
            new MatsimNetworkReader(scenario.getNetwork()).readFile(localPath);
        } catch (Exception e) {
            System.out.println("SKIPPED: Berlin v7.0 network could not be parsed: " + e.getMessage());
            return;
        }

        Network network = scenario.getNetwork();
        int nodeCount = network.getNodes().size();
        int linkCount = network.getLinks().size();
        System.out.printf("%nBerlin v7.0 network: %,d nodes, %,d links%n", nodeCount, linkCount);

        // Benchmark
        benchmarkNetwork("Berlin-v7.0", network);

        // Correctness test
        runCorrectnessTest(network);
    }

    // -----------------------------------------------------------------------
    // Helper: run correctness test (compare ND-CH against Dijkstra)
    // -----------------------------------------------------------------------

    private void runCorrectnessTest(Network network) {
        FreespeedTravelTimeAndDisutility tc =
                new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

        SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);

        // Build ND-ordered CH
        int[] order = new InertialFlowCutter(baseGraph).computeOrder();
        SpeedyCHGraph chGraph = new SpeedyCHBuilder(baseGraph, tc).buildWithOrder(order);
        new SpeedyCHTTFCustomizer().customize(chGraph, tc, tc);
        SpeedyCHTimeDep chRouter = new SpeedyCHTimeDep(chGraph, tc, tc);

        // Reference: Dijkstra
        SpeedyDijkstra dijkstra = new SpeedyDijkstra(baseGraph, tc, tc);

        List<Node> nodeList = new ArrayList<>(network.getNodes().values());
        int n = nodeList.size();
        if (n < 2) return;

        Random rng = new Random(42);
        int mismatches = 0;

        for (int i = 0; i < NUM_QUERIES; i++) {
            Node src = nodeList.get(rng.nextInt(n));
            Node dst = nodeList.get(rng.nextInt(n));

            Path chPath  = chRouter.calcLeastCostPath(src, dst, 8.0 * 3600, null, null);
            Path dijPath = dijkstra.calcLeastCostPath(src, dst, 8.0 * 3600, null, null);

            if (chPath == null && dijPath == null) continue;

            Assertions.assertNotNull(chPath,
                    "ND-CH null but Dijkstra found path " + src.getId() + "→" + dst.getId());
            Assertions.assertNotNull(dijPath,
                    "Dijkstra null but ND-CH found path " + src.getId() + "→" + dst.getId());

            if (Math.abs(chPath.travelCost - dijPath.travelCost) > COST_TOLERANCE) {
                mismatches++;
                System.err.printf("MISMATCH %s→%s: ND-CH=%.6f  Dijkstra=%.6f%n",
                        src.getId(), dst.getId(), chPath.travelCost, dijPath.travelCost);
            }
        }

        Assertions.assertEquals(0, mismatches,
                mismatches + " cost mismatches out of " + NUM_QUERIES + " queries.");
    }

    // -----------------------------------------------------------------------
    // Helper: benchmark a single network
    // -----------------------------------------------------------------------

    private void benchmarkNetwork(String name, Network network) {
        if (network == null) return;

        FreespeedTravelTimeAndDisutility tc =
                new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        int nodeCount = network.getNodes().size();
        int linkCount = network.getLinks().size();

        // ---- Witness-based CH build ----
        SpeedyGraph gW = SpeedyGraphBuilder.build(network);
        // Warm up
        new SpeedyCHBuilder(gW, tc).build();
        // Timed run
        gW = SpeedyGraphBuilder.build(network);
        long witnessStart = System.nanoTime();
        SpeedyCHGraph chW = new SpeedyCHBuilder(gW, tc).build();
        long witnessMs = (System.nanoTime() - witnessStart) / 1_000_000;

        // ---- ND-ordered CH build ----
        SpeedyGraph gN = SpeedyGraphBuilder.build(network);
        // Warm up
        {
            int[] warmOrder = new InertialFlowCutter(gN).computeOrder();
            new SpeedyCHBuilder(gN, tc).buildWithOrder(warmOrder);
        }
        // Timed run
        gN = SpeedyGraphBuilder.build(network);
        long ndStart = System.nanoTime();
        long ndOrdStart = System.nanoTime();
        int[] order = new InertialFlowCutter(gN).computeOrder();
        long ndOrdMs = (System.nanoTime() - ndOrdStart) / 1_000_000;
        long ndConStart = System.nanoTime();
        SpeedyCHGraph chN = new SpeedyCHBuilder(gN, tc).buildWithOrder(order);
        long ndConMs = (System.nanoTime() - ndConStart) / 1_000_000;
        long ndTotalMs = (System.nanoTime() - ndStart) / 1_000_000;

        double edgeOverhead = ((double) chN.totalEdgeCount / Math.max(1, chW.totalEdgeCount) - 1) * 100;

        // ---- Query performance (ND-CH vs Dijkstra) ----
        new SpeedyCHTTFCustomizer().customize(chN, tc, tc);
        SpeedyCHTimeDep ndRouter = new SpeedyCHTimeDep(chN, tc, tc);
        SpeedyDijkstra dijkstra = new SpeedyDijkstra(gN, tc, tc);

        List<Node> nodeList = new ArrayList<>(network.getNodes().values());
        int n = nodeList.size();
        Random rng = new Random(42);
        int queryCount = Math.min(200, NUM_QUERIES);

        // Warm up queries
        for (int i = 0; i < 20; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));
            ndRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            dijkstra.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
        }
        rng = new Random(123);  // fresh seed for timing

        long dijkTimeNs = 0;
        long chTimeNs = 0;
        for (int i = 0; i < queryCount; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));

            long t0 = System.nanoTime();
            dijkstra.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            dijkTimeNs += System.nanoTime() - t0;

            t0 = System.nanoTime();
            ndRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            chTimeNs += System.nanoTime() - t0;
        }
        double querySpeedup = (double) dijkTimeNs / Math.max(1, chTimeNs);

        System.out.printf("║ %-22s │ %5d │ %5d │ %7d   │ %7d   │ %6d   │ %6d   │ %+7.1f%%  │    %5.1fx      ║%n",
                name, nodeCount, linkCount, witnessMs, ndTotalMs, ndOrdMs, ndConMs, edgeOverhead, querySpeedup);
    }

    // -----------------------------------------------------------------------
    // Helper: load network from file
    // -----------------------------------------------------------------------

    private static Network loadNetwork(String path) {
        Scenario s = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(s.getNetwork()).readFile(path);
        return s.getNetwork();
    }

    // -----------------------------------------------------------------------
    // Helper: build large perturbed grid network
    // -----------------------------------------------------------------------

    /**
     * Builds an n×n grid with randomly perturbed coordinates and variable link speeds.
     * This is much more realistic than a regular grid for testing graph partitioning.
     *
     * @param n    grid dimension (n×n nodes)
     * @param seed random seed for reproducibility
     */
    private static Network buildPerturbedGrid(int n, long seed) {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();
        Random rng = new Random(seed);

        double spacing = 200.0; // 200m base spacing

        Node[][] nodes = new Node[n][n];
        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n; c++) {
                // Perturb coordinates by up to 30% of spacing
                double px = c * spacing + (rng.nextDouble() - 0.5) * spacing * 0.6;
                double py = r * spacing + (rng.nextDouble() - 0.5) * spacing * 0.6;
                Node node = nf.createNode(Id.createNodeId(r + "_" + c), new Coord(px, py));
                network.addNode(node);
                nodes[r][c] = node;
            }
        }

        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n; c++) {
                // Horizontal links (right + left)
                if (c + 1 < n) {
                    double len = distance(nodes[r][c], nodes[r][c + 1]);
                    double speed = 8.0 + rng.nextDouble() * 22.0; // 8-30 m/s
                    addLink(network, r + "_" + c + "R", nodes[r][c], nodes[r][c + 1], len, speed);
                    addLink(network, r + "_" + c + "L", nodes[r][c + 1], nodes[r][c], len, speed);
                }
                // Vertical links (down + up)
                if (r + 1 < n) {
                    double len = distance(nodes[r][c], nodes[r + 1][c]);
                    double speed = 8.0 + rng.nextDouble() * 22.0;
                    addLink(network, r + "_" + c + "D", nodes[r][c], nodes[r + 1][c], len, speed);
                    addLink(network, r + "_" + c + "U", nodes[r + 1][c], nodes[r][c], len, speed);
                }
            }
        }
        return network;
    }

    private static double distance(Node a, Node b) {
        double dx = a.getCoord().getX() - b.getCoord().getX();
        double dy = a.getCoord().getY() - b.getCoord().getY();
        return Math.max(1.0, Math.sqrt(dx * dx + dy * dy));
    }

    private static void addLink(Network network, String id, Node from, Node to,
                                double length, double freespeed) {
        NetworkFactory nf = network.getFactory();
        Link link = nf.createLink(Id.createLinkId(id), from, to);
        link.setLength(length);
        link.setFreespeed(freespeed);
        link.setCapacity(1800);
        link.setNumberOfLanes(1);
        network.addLink(link);
    }
}
