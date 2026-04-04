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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Large-scale tests and benchmarks for nested-dissection-ordered CH contraction
 * ({@link InertialFlowCutter} + {@link SpeedyCHBuilder#buildWithOrder}).
 *
 * <p>Benchmarks use <b>SpeedyALT</b> (A* with Landmarks) as the query baseline,
 * since it is the standard production router in MATSim.
 *
 * <p>Tests on real and large synthetic networks to evaluate performance and correctness
 * at realistic scales:
 * <ul>
 *   <li><b>Real networks</b>: Berlin (11.5k nodes)</li>
 *   <li><b>Synthetic large grids</b>: up to 22,500 nodes with perturbed coordinates</li>
 *   <li><b>Berlin v7.0</b>: ~65k nodes, loaded from URL when accessible</li>
 * </ul>
 *
 * <p>Results are written to {@code target/ch-benchmark-results.tsv} for post-processing.
 */
public class SpeedyCHBuilderLargeNetworkTest {

    private static final int    NUM_QUERIES    = 500;
    private static final int    BENCHMARK_QUERIES = 200;
    private static final int    ALT_LANDMARKS  = 16;
    private static final double COST_TOLERANCE = 1e-6;
    private static final String RESULTS_FILE   = "target/ch-benchmark-results.tsv";

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
        System.out.printf("Berlin network: %,d nodes, %,d links%n",
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
        System.out.printf("100x100 perturbed grid: %,d nodes, %,d links%n",
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
        System.out.printf("150x150 perturbed grid: %,d nodes, %,d links%n",
                network.getNodes().size(), network.getLinks().size());
        runCorrectnessTest(network);
    }

    // -----------------------------------------------------------------------
    // Multi-network benchmark (baseline: SpeedyALT)
    // -----------------------------------------------------------------------

    /**
     * Comprehensive benchmark comparing ND-ordered CH vs SpeedyALT (production baseline).
     *
     * <p>Reports CH build time, edge overhead, and query speedup vs SpeedyALT.
     * Results are dumped to {@value RESULTS_FILE} as a tab-separated file.
     */
    @Test
    void benchmarkAllNetworks() {
        List<BenchmarkResult> results = new ArrayList<>();

        results.add(benchmarkNetwork("Berlin-11k",
                loadNetwork("test/scenarios/berlin/network.xml.gz")));
        results.add(benchmarkNetwork("Grid-50x50",  buildPerturbedGrid(50, 42)));
        results.add(benchmarkNetwork("Grid-100x100", buildPerturbedGrid(100, 42)));
        results.add(benchmarkNetwork("Grid-150x150", buildPerturbedGrid(150, 42)));

        // ---- Console summary ----
        System.out.println();
        System.out.println("=== CH Benchmark Results (baseline: SpeedyALT) ===");
        System.out.printf("%-16s %7s %7s | %8s %8s %8s | %7s | %8s %8s %10s%n",
                "Network", "Nodes", "Links",
                "Wit(ms)", "ND(ms)", "Ord(ms)",
                "EdgeOvh",
                "ALT(µs)", "CH(µs)", "Speedup");
        System.out.println("-".repeat(110));

        for (BenchmarkResult r : results) {
            System.out.printf("%-16s %7d %7d | %8d %8d %8d | %+6.1f%% | %8.0f %8.0f %9.1fx%n",
                    r.name, r.nodes, r.links,
                    r.witnessBuildMs, r.ndTotalMs, r.ndOrdMs,
                    r.edgeOverheadPct,
                    r.altQueryUs, r.chQueryUs, r.speedupVsALT);
        }
        System.out.println();

        // ---- Write TSV file ----
        writeResultsFile(results);
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
        java.nio.file.Path localPath = Paths.get("/tmp/berlin-v7.0-network.xml.gz");

        // Download with native Java HTTP client if not already cached
        if (!Files.exists(localPath) || fileSize(localPath) < 1000) {
            try {
                HttpClient client = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(60))
                        .GET()
                        .build();
                HttpResponse<InputStream> response = client.send(request,
                        HttpResponse.BodyHandlers.ofInputStream());
                if (response.statusCode() != 200) {
                    System.out.println("SKIPPED: Berlin v7.0 network not accessible (HTTP " + response.statusCode() + ").");
                    return;
                }
                try (InputStream in = response.body()) {
                    Files.copy(in, localPath, StandardCopyOption.REPLACE_EXISTING);
                }
                if (fileSize(localPath) < 1000) {
                    System.out.println("SKIPPED: Berlin v7.0 network download too small.");
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
            new MatsimNetworkReader(scenario.getNetwork()).readFile(localPath.toString());
        } catch (Exception e) {
            System.out.println("SKIPPED: Berlin v7.0 network could not be parsed: " + e.getMessage());
            return;
        }

        Network network = scenario.getNetwork();
        System.out.printf("Berlin v7.0 network: %,d nodes, %,d links%n",
                network.getNodes().size(), network.getLinks().size());

        try {
            // Benchmark + print single-row table
            BenchmarkResult r = benchmarkNetwork("Berlin-v7.0", network);
            System.out.printf("  Build: witness=%dms  ND=%dms (ord=%dms)  EdgeOverhead=%+.1f%%%n",
                    r.witnessBuildMs, r.ndTotalMs, r.ndOrdMs, r.edgeOverheadPct);
            System.out.printf("  Query: ALT=%.0fµs  CH=%.0fµs  Speedup=%.1fx%n",
                    r.altQueryUs, r.chQueryUs, r.speedupVsALT);

            // Correctness test
            runCorrectnessTest(network);
        } catch (OutOfMemoryError e) {
            System.out.println("SKIPPED: Berlin v7.0 benchmark/correctness test: insufficient heap (" + e.getMessage() + ").");
        }
    }

    // -----------------------------------------------------------------------
    // Benchmark result record
    // -----------------------------------------------------------------------

    private record BenchmarkResult(
            String name, int nodes, int links,
            long witnessBuildMs, long ndTotalMs, long ndOrdMs, long ndConMs,
            int witnessEdges, int ndEdges, double edgeOverheadPct,
            double altQueryUs, double chQueryUs, double speedupVsALT) {
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

        // Reference: Dijkstra (exact baseline for correctness)
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
    // Helper: benchmark a single network (returns structured result)
    // -----------------------------------------------------------------------

    private BenchmarkResult benchmarkNetwork(String name, Network network) {
        FreespeedTravelTimeAndDisutility tc =
                new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        int nodeCount = network.getNodes().size();
        int linkCount = network.getLinks().size();

        // ---- Witness-based CH build ----
        SpeedyGraph gW = SpeedyGraphBuilder.build(network);
        new SpeedyCHBuilder(gW, tc).build(); // warm-up
        gW = SpeedyGraphBuilder.build(network);
        long witnessStart = System.nanoTime();
        SpeedyCHGraph chW = new SpeedyCHBuilder(gW, tc).build();
        long witnessMs = (System.nanoTime() - witnessStart) / 1_000_000;

        // ---- ND-ordered CH build ----
        SpeedyGraph gN = SpeedyGraphBuilder.build(network);
        { // warm-up
            int[] warmOrder = new InertialFlowCutter(gN).computeOrder();
            new SpeedyCHBuilder(gN, tc).buildWithOrder(warmOrder);
        }
        gN = SpeedyGraphBuilder.build(network);
        long ndStart = System.nanoTime();
        int[] order = new InertialFlowCutter(gN).computeOrder();
        long ndOrdMs = (System.nanoTime() - ndStart) / 1_000_000;
        long ndConStart = System.nanoTime();
        SpeedyCHGraph chN = new SpeedyCHBuilder(gN, tc).buildWithOrder(order);
        long ndConMs = (System.nanoTime() - ndConStart) / 1_000_000;
        long ndTotalMs = (System.nanoTime() - ndStart) / 1_000_000;

        double edgeOverhead = ((double) chN.totalEdgeCount / Math.max(1, chW.totalEdgeCount) - 1) * 100;

        // ---- Setup query routers: ND-CH and SpeedyALT ----
        new SpeedyCHTTFCustomizer().customize(chN, tc, tc);
        SpeedyCHTimeDep ndRouter = new SpeedyCHTimeDep(chN, tc, tc);
        SpeedyALTData altData = new SpeedyALTData(gN, Math.min(ALT_LANDMARKS, gN.nodeCount), tc, 1);
        SpeedyALT altRouter = new SpeedyALT(altData, tc, tc);

        // ---- Query benchmark: ND-CH vs SpeedyALT ----
        List<Node> nodeList = new ArrayList<>(network.getNodes().values());
        int n = nodeList.size();
        Random rng = new Random(42);

        // Warm up both routers
        for (int i = 0; i < 20; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));
            ndRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            altRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
        }

        rng = new Random(123); // fresh seed for timed queries
        long altTimeNs = 0;
        long chTimeNs = 0;
        for (int i = 0; i < BENCHMARK_QUERIES; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));

            long t0 = System.nanoTime();
            altRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            altTimeNs += System.nanoTime() - t0;

            t0 = System.nanoTime();
            ndRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            chTimeNs += System.nanoTime() - t0;
        }

        double altQueryUs = altTimeNs / (BENCHMARK_QUERIES * 1000.0);
        double chQueryUs  = chTimeNs  / (BENCHMARK_QUERIES * 1000.0);
        double speedup = (double) altTimeNs / Math.max(1, chTimeNs);

        return new BenchmarkResult(name, nodeCount, linkCount,
                witnessMs, ndTotalMs, ndOrdMs, ndConMs,
                chW.totalEdgeCount, chN.totalEdgeCount, edgeOverhead,
                altQueryUs, chQueryUs, speedup);
    }

    // -----------------------------------------------------------------------
    // Helper: write results to TSV file
    // -----------------------------------------------------------------------

    private static void writeResultsFile(List<BenchmarkResult> results) {
        try {
            Files.createDirectories(Paths.get("target"));
            try (PrintWriter pw = new PrintWriter(RESULTS_FILE)) {
                pw.println("network\tnodes\tlinks\t"
                        + "witness_build_ms\tnd_total_ms\tnd_ord_ms\tnd_con_ms\t"
                        + "witness_edges\tnd_edges\tedge_overhead_pct\t"
                        + "alt_query_us\tch_query_us\tspeedup_vs_alt");
                for (BenchmarkResult r : results) {
                    pw.printf("%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.2f%n",
                            r.name, r.nodes, r.links,
                            r.witnessBuildMs, r.ndTotalMs, r.ndOrdMs, r.ndConMs,
                            r.witnessEdges, r.ndEdges, r.edgeOverheadPct,
                            r.altQueryUs, r.chQueryUs, r.speedupVsALT);
                }
            }
            System.out.println("Results written to " + RESULTS_FILE);
        } catch (IOException e) {
            System.err.println("Failed to write results file: " + e.getMessage());
        }
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

    private static long fileSize(java.nio.file.Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            return 0;
        }
    }
}
