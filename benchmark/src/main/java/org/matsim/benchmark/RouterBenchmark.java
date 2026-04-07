package org.matsim.benchmark;

import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.NetworkFactory;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.router.AStarLandmarksFactory;
import org.matsim.core.router.DijkstraFactory;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.speedy.SpeedyALTFactory;
import org.matsim.core.router.speedy.CHBuilder;
import org.matsim.core.router.speedy.CHGraph;
import org.matsim.core.router.speedy.CHTTFCustomizer;
import org.matsim.core.router.speedy.CHRouterTimeDep;
import org.matsim.core.router.speedy.SpeedyDijkstra;
import org.matsim.core.router.speedy.SpeedyGraph;
import org.matsim.core.router.speedy.SpeedyGraphBuilder;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.LeastCostPathCalculator.Path;
import org.matsim.core.scenario.ScenarioUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Synthetic routing benchmark comparing all available MATSim routing algorithms:
 * <ol>
 *   <li>Dijkstra (classic, no preprocessing)</li>
 *   <li>AStarLandmarks (classic, with landmark preprocessing)</li>
 *   <li>SpeedyDijkstra (optimised, no preprocessing)</li>
 *   <li>SpeedyALT (optimised A*-Landmarks)</li>
 *   <li>CHRouterTimeDep (time-dependent CATCHUp CH router)</li>
 * </ol>
 *
 * <p>The benchmark generates a configurable N×N bidirectional grid network,
 * draws {@value #NUM_QUERIES} random OD pairs and measures:
 * <ul>
 *   <li>Preprocessing / build time (ms)</li>
 *   <li>Total query time (ms) after a warm-up pass</li>
 *   <li>Average query time (µs)</li>
 *   <li>Throughput (queries / second)</li>
 * </ul>
 *
 * <p>Run with {@code java -cp ... org.matsim.benchmark.RouterBenchmark [gridSize] [numQueries]}
 */
public class RouterBenchmark {

    /** Default grid side length (N×N nodes, 2×N×(N-1) bidirectional links). */
    private static final int DEFAULT_GRID_SIZE = 50;

    /** Number of random OD pairs used for timing. */
    private static final int NUM_QUERIES = 10_000;

    /** Number of warm-up queries (not included in timing). */
    private static final int WARMUP_QUERIES = 500;

    /** Link length in metres. */
    private static final double LINK_LENGTH = 1_000.0;

    /** Link free-speed in m/s. */
    private static final double LINK_FREESPEED = 13.89; // 50 km/h

    public static void main(String[] args) {
        int gridSize   = DEFAULT_GRID_SIZE;
        int numQueries = NUM_QUERIES;

        if (args.length >= 1) gridSize   = Integer.parseInt(args[0]);
        if (args.length >= 2) numQueries = Integer.parseInt(args[1]);

        new RouterBenchmark().run(gridSize, numQueries);
    }

    private void run(int gridSize, int numQueries) {
        System.out.printf("=== MATSim Routing Benchmark ===%n");
        System.out.printf("Grid: %d×%d  |  Links: %d  |  Queries: %d%n%n",
                gridSize, gridSize,
                2 * gridSize * (gridSize - 1),
                numQueries);

        Network network = buildGridNetwork(gridSize);
        FreespeedTravelTimeAndDisutility tc =
                new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

        List<Node> nodes = new ArrayList<>(network.getNodes().values());
        Random rng = new Random(12345);

        // Pre-generate OD pairs so all algorithms use identical queries.
        int n = nodes.size();
        int[] srcIdx = new int[numQueries + WARMUP_QUERIES];
        int[] dstIdx = new int[numQueries + WARMUP_QUERIES];
        for (int i = 0; i < srcIdx.length; i++) {
            srcIdx[i] = rng.nextInt(n);
            dstIdx[i] = rng.nextInt(n);
        }

        printHeader();

        // ---- 1. Dijkstra (classic) ----
        {
            long t0 = System.nanoTime();
            LeastCostPathCalculator router =
                    new DijkstraFactory().createPathCalculator(network, tc, tc);
            long prepMs = ns2ms(System.nanoTime() - t0);
            benchmark("Dijkstra", router, nodes, srcIdx, dstIdx, numQueries, prepMs);
        }

        // ---- 2. AStarLandmarks (classic) ----
        {
            long t0 = System.nanoTime();
            LeastCostPathCalculator router =
                    new AStarLandmarksFactory(4, 16).createPathCalculator(network, tc, tc);
            long prepMs = ns2ms(System.nanoTime() - t0);
            benchmark("AStarLandmarks", router, nodes, srcIdx, dstIdx, numQueries, prepMs);
        }

        // ---- 3. SpeedyDijkstra ----
        {
            long t0 = System.nanoTime();
            SpeedyGraph g = SpeedyGraphBuilder.build(network);
            LeastCostPathCalculator router = new SpeedyDijkstra(g, tc, tc);
            long prepMs = ns2ms(System.nanoTime() - t0);
            benchmark("SpeedyDijkstra", router, nodes, srcIdx, dstIdx, numQueries, prepMs);
        }

        // ---- 4. SpeedyALT ----
        {
            long t0 = System.nanoTime();
            LeastCostPathCalculator router =
                    new SpeedyALTFactory(4, 16).createPathCalculator(network, tc, tc);
            long prepMs = ns2ms(System.nanoTime() - t0);
            benchmark("SpeedyALT", router, nodes, srcIdx, dstIdx, numQueries, prepMs);
        }

        // ---- 5. CHRouterTimeDep (CATCHUp) ----
        {
            long t0 = System.nanoTime();
            SpeedyGraph g = SpeedyGraphBuilder.build(network);
            CHGraph chGraph = new CHBuilder(g, tc).build();
            new CHTTFCustomizer().customize(chGraph, tc, tc);
            LeastCostPathCalculator router = new CHRouterTimeDep(chGraph, tc, tc);
            long prepMs = ns2ms(System.nanoTime() - t0);
            benchmark("CHRouterTimeDep", router, nodes, srcIdx, dstIdx, numQueries, prepMs);
        }

        System.out.println();
    }

    // -------------------------------------------------------------------------
    // Core benchmark loop
    // -------------------------------------------------------------------------

    private void benchmark(String name, LeastCostPathCalculator router,
                           List<Node> nodes,
                           int[] srcIdx, int[] dstIdx,
                           int numQueries, long prepMs) {
        // Warm-up (excluded from timing).
        for (int i = 0; i < WARMUP_QUERIES; i++) {
            router.calcLeastCostPath(nodes.get(srcIdx[i]), nodes.get(dstIdx[i]),
                    8.0 * 3600, null, null);
        }

        // Timed queries.
        long t0 = System.nanoTime();
        int nullCount = 0;
        for (int i = WARMUP_QUERIES; i < WARMUP_QUERIES + numQueries; i++) {
            Path p = router.calcLeastCostPath(nodes.get(srcIdx[i]), nodes.get(dstIdx[i]),
                    8.0 * 3600, null, null);
            if (p == null) nullCount++;
        }
        long queryNs = System.nanoTime() - t0;
        long queryMs = ns2ms(queryNs);
        double avgUs = queryNs / 1_000.0 / numQueries;
        double qps   = numQueries * 1_000_000_000.0 / queryNs;

        System.out.printf("%-20s  %8d ms  %8d ms  %10.2f µs  %12.0f q/s  %s%n",
                name, prepMs, queryMs, avgUs, qps,
                nullCount > 0 ? "(+" + nullCount + " null)" : "");
    }

    private static void printHeader() {
        System.out.printf("%-20s  %10s  %10s  %13s  %14s%n",
                "Algorithm", "Prep (ms)", "Query (ms)", "Avg (µs)", "Throughput");
        System.out.println("-".repeat(75));
    }

    private static long ns2ms(long ns) {
        return ns / 1_000_000;
    }

    // -------------------------------------------------------------------------
    // Network builder
    // -------------------------------------------------------------------------

    /**
     * Builds an N×N fully bidirectional grid network.
     * Nodes are labelled {@code r_c}; links {@code r_cR/L/D/U}.
     */
    public static Network buildGridNetwork(int n) {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();

        Node[][] nodeArr = new Node[n][n];
        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n; c++) {
                Node node = nf.createNode(
                        Id.createNodeId(r + "_" + c),
                        new Coord(c * LINK_LENGTH, r * LINK_LENGTH));
                network.addNode(node);
                nodeArr[r][c] = node;
            }
        }

        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n; c++) {
                if (c + 1 < n) {
                    addLink(network, r + "_" + c + "R", nodeArr[r][c],     nodeArr[r][c + 1]);
                    addLink(network, r + "_" + c + "L", nodeArr[r][c + 1], nodeArr[r][c]);
                }
                if (r + 1 < n) {
                    addLink(network, r + "_" + c + "D", nodeArr[r][c],     nodeArr[r + 1][c]);
                    addLink(network, r + "_" + c + "U", nodeArr[r + 1][c], nodeArr[r][c]);
                }
            }
        }
        return network;
    }

    private static void addLink(Network network, String id, Node from, Node to) {
        NetworkFactory nf = network.getFactory();
        Link link = nf.createLink(Id.createLinkId(id), from, to);
        link.setLength(LINK_LENGTH);
        link.setFreespeed(LINK_FREESPEED);
        link.setCapacity(1800);
        link.setNumberOfLanes(1);
        network.addLink(link);
    }
}
