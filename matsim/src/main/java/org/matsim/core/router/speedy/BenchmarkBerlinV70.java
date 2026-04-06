/* *********************************************************************** *
 * project: org.matsim.*
 * BenchmarkBerlinV70.java
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2025 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */
package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.LeastCostPathCalculator.Path;
import org.matsim.core.scenario.ScenarioUtils;

import java.io.InputStream;
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
 * Standalone benchmark comparing <b>router variants</b> on the Berlin v7.0
 * network (~88k nodes, ~200k links):
 * <ol>
 *   <li>SpeedyALT</li>
 *   <li>CH Time-Dependent</li>
 * </ol>
 *
 * <p>Run with sufficient heap, e.g.:
 * <pre>
 *   java -Xmx8G -cp ... org.matsim.core.router.speedy.BenchmarkBerlinV70
 * </pre>
 *
 * <p>The network is downloaded from the TU Berlin SVN and cached in the
 * system temp directory.
 *
 * @author Steffen Axer
 */
public class BenchmarkBerlinV70 {

    private static final String NETWORK_URL =
            "https://svn.vsp.tu-berlin.de/repos/public-svn/matsim/scenarios/countries/de/berlin/"
                    + "berlin-v7.0/input/berlin-v7.0-network.xml.gz";

    private static final int WARMUP_QUERIES    = 200;
    private static final int BENCHMARK_QUERIES = 2_000;
    private static final int ALT_LANDMARKS     = 16;

    public static void main(String[] args) {
        long maxHeapMB = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        System.out.printf("JVM max heap: %,d MB%n", maxHeapMB);
        if (maxHeapMB < 3000) {
            System.err.println("ERROR: Need at least -Xmx4G. Current max heap: " + maxHeapMB + " MB.");
            System.err.println("  Usage: java -Xmx8G -cp <classpath> " + BenchmarkBerlinV70.class.getName());
            System.exit(1);
        }

        // ---- 1. Download / load network ----
        Network network = downloadAndLoadNetwork();
        System.out.printf("Berlin v7.0 network: %,d nodes, %,d links%n",
                network.getNodes().size(), network.getLinks().size());

        FreespeedTravelTimeAndDisutility tc =
                new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

        // ---- 2a. Build graph WITH Morton Z-order spatial reordering ----
        System.out.println();
        System.out.println("Building SpeedyGraph (Morton Z-order) ...");
        SpeedyGraph graphZ = SpeedyGraphBuilder.build(network);

        // ---- 2b. Build graph WITHOUT spatial reordering (identity ordering) ----
        System.out.println("Building SpeedyGraph (identity ordering) ...");
        SpeedyGraph graphId = SpeedyGraphBuilder.buildWithIdentityOrdering(network);

        // ---- 3. Compute ND order (once per graph, reuse for CH) ----
        System.out.println();
        System.out.println("Computing InertialFlowCutter ND order (Z-order graph) ...");
        long t0 = System.nanoTime();
        InertialFlowCutter.NDOrderResult orderZ = new InertialFlowCutter(graphZ).computeOrderWithBatches();
        long orderZMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("  ND Order (Z):   %,6d ms%n", orderZMs);

        System.out.println("Computing InertialFlowCutter ND order (identity graph) ...");
        long t0b = System.nanoTime();
        InertialFlowCutter.NDOrderResult orderId = new InertialFlowCutter(graphId).computeOrderWithBatches();
        long orderIdMs = (System.nanoTime() - t0b) / 1_000_000;
        System.out.printf("  ND Order (id):  %,6d ms%n", orderIdMs);

        // ---- 4. Build CH for both graphs ----
        System.out.println();
        System.out.println("Building CH (Z-order graph) ...");
        long t1 = System.nanoTime();
        CHGraph chGraphZ = new CHBuilder(graphZ, tc).buildWithOrderParallel(orderZ);
        long contrZMs = (System.nanoTime() - t1) / 1_000_000;
        new CHTTFCustomizer().customize(chGraphZ, tc, tc);
        long totalBuildZMs = orderZMs + (System.nanoTime() - t1) / 1_000_000;
        System.out.printf("  CH build (Z):   %,6d ms  (%,d edges)%n", totalBuildZMs, chGraphZ.totalEdgeCount);

        System.out.println("Building CH (identity graph) ...");
        long t2 = System.nanoTime();
        CHGraph chGraphId = new CHBuilder(graphId, tc).buildWithOrderParallel(orderId);
        long contrIdMs = (System.nanoTime() - t2) / 1_000_000;
        new CHTTFCustomizer().customize(chGraphId, tc, tc);
        long totalBuildIdMs = orderIdMs + (System.nanoTime() - t2) / 1_000_000;
        System.out.printf("  CH build (id):  %,6d ms  (%,d edges)%n", totalBuildIdMs, chGraphId.totalEdgeCount);

        // ---- 5. Build ALT data for both graphs ----
        System.out.println();
        System.out.println("Building SpeedyALT landmarks ...");
        long altZStart = System.nanoTime();
        SpeedyALTData altDataZ = new SpeedyALTData(graphZ, Math.min(ALT_LANDMARKS, graphZ.nodeCount), tc, 1);
        long altZMs = (System.nanoTime() - altZStart) / 1_000_000;
        System.out.printf("  ALT build (Z):  %,6d ms  (%d landmarks)%n", altZMs, ALT_LANDMARKS);

        long altIdStart = System.nanoTime();
        SpeedyALTData altDataId = new SpeedyALTData(graphId, Math.min(ALT_LANDMARKS, graphId.nodeCount), tc, 1);
        long altIdMs = (System.nanoTime() - altIdStart) / 1_000_000;
        System.out.printf("  ALT build (id): %,6d ms  (%d landmarks)%n", altIdMs, ALT_LANDMARKS);

        // ---- 6. Create routers ----
        SpeedyALT altZ       = new SpeedyALT(altDataZ, tc, tc);
        SpeedyALT altId      = new SpeedyALT(altDataId, tc, tc);
        CHRouterTimeDep chZ   = new CHRouterTimeDep(chGraphZ, tc, tc);
        CHRouterTimeDep chId  = new CHRouterTimeDep(chGraphId, tc, tc);

        List<Node> nodeList = new ArrayList<>(network.getNodes().values());
        int n = nodeList.size();

        // ---- 7. Warm-up ----
        System.out.println();
        System.out.printf("Warming up (%d queries per router) ...%n", WARMUP_QUERIES);
        Random rng = new Random(42);
        for (int i = 0; i < WARMUP_QUERIES; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));
            double depTime = 8.0 * 3600;
            altZ.calcLeastCostPath(s, d, depTime, null, null);
            altId.calcLeastCostPath(s, d, depTime, null, null);
            chZ.calcLeastCostPath(s, d, depTime, null, null);
            chId.calcLeastCostPath(s, d, depTime, null, null);
        }

        // ---- 8. Benchmark ----
        System.out.printf("Running benchmark (%,d queries per router) ...%n", BENCHMARK_QUERIES);
        rng = new Random(123);

        // Pre-generate random pairs so all routers use the exact same queries
        int[][] pairs = new int[BENCHMARK_QUERIES][2];
        for (int i = 0; i < BENCHMARK_QUERIES; i++) {
            pairs[i][0] = rng.nextInt(n);
            pairs[i][1] = rng.nextInt(n);
        }

        // Benchmark each router separately to avoid interleaved GC interference
        long altIdNs = benchmarkRouter(altId, nodeList, pairs, "ALT (identity)");
        long altZNs  = benchmarkRouter(altZ,  nodeList, pairs, "ALT (Z-order)");
        long chIdNs  = benchmarkRouter(chId,  nodeList, pairs, "CH  (identity)");
        long chZNs   = benchmarkRouter(chZ,   nodeList, pairs, "CH  (Z-order)");

        // Quick correctness check: compare ALT(Z) vs CH(Z) and ALT(id) vs ALT(Z)
        int mismatches = 0;
        double maxCostDiff = 0;
        for (int i = 0; i < Math.min(200, BENCHMARK_QUERIES); i++) {
            Node s = nodeList.get(pairs[i][0]);
            Node d = nodeList.get(pairs[i][1]);
            Path pathALTz  = altZ.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            Path pathCHz   = chZ.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            Path pathALTid = altId.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            if (pathALTz != null && pathCHz != null) {
                double diff = Math.abs(pathALTz.travelCost - pathCHz.travelCost);
                maxCostDiff = Math.max(maxCostDiff, diff);
                if (diff > 1e-3) {
                    mismatches++;
                    if (mismatches <= 5) {
                        System.err.printf("  MISMATCH #%d: %s->%s  ALT(Z)=%.4f  CH(Z)=%.4f  diff=%.6f%n",
                                mismatches, s.getId(), d.getId(),
                                pathALTz.travelCost, pathCHz.travelCost, diff);
                    }
                }
            }
            // Also check that ALT(identity) and ALT(Z-order) give the same result
            if (pathALTz != null && pathALTid != null) {
                double diff = Math.abs(pathALTz.travelCost - pathALTid.travelCost);
                maxCostDiff = Math.max(maxCostDiff, diff);
                if (diff > 1e-3) {
                    mismatches++;
                    if (mismatches <= 5) {
                        System.err.printf("  MISMATCH #%d: %s->%s  ALT(Z)=%.4f  ALT(id)=%.4f  diff=%.6f%n",
                                mismatches, s.getId(), d.getId(),
                                pathALTz.travelCost, pathALTid.travelCost, diff);
                    }
                }
            }
        }

        // ---- 9. Results ----
        double altIdUs   = altIdNs / (BENCHMARK_QUERIES * 1000.0);
        double altZUs    = altZNs  / (BENCHMARK_QUERIES * 1000.0);
        double chIdUs    = chIdNs  / (BENCHMARK_QUERIES * 1000.0);
        double chZUs     = chZNs   / (BENCHMARK_QUERIES * 1000.0);
        double altSpeedup = altIdUs / Math.max(0.001, altZUs);
        double chSpeedup  = chIdUs  / Math.max(0.001, chZUs);
        double edgeOverhead = ((double) chGraphZ.totalEdgeCount / network.getLinks().size() - 1) * 100;

        System.out.println();
        printBox("Berlin v7.0  —  Z-Order vs Identity Comparison", new String[][] {
                { "Network" },
                { "  Nodes",           String.format("%,d", network.getNodes().size()) },
                { "  Links",           String.format("%,d", network.getLinks().size()) },
                { "  CH edges",        String.format("%,d  (%+.1f%% overhead)", chGraphZ.totalEdgeCount, edgeOverhead) },
                null, // separator
                { "Preprocessing" },
                { "  ND Order (Z)",    String.format("%,d ms", orderZMs) },
                { "  ND Order (id)",   String.format("%,d ms", orderIdMs) },
                { "  CH build (Z)",    String.format("%,d ms  (incl. order)", totalBuildZMs) },
                { "  CH build (id)",   String.format("%,d ms  (incl. order)", totalBuildIdMs) },
                { "  ALT build (Z)",   String.format("%,d ms  (%d landmarks)", altZMs, ALT_LANDMARKS) },
                { "  ALT build (id)",  String.format("%,d ms  (%d landmarks)", altIdMs, ALT_LANDMARKS) },
                null,
                { "Query Performance", String.format("(%,d queries, %d warmup)", BENCHMARK_QUERIES, WARMUP_QUERIES) },
                { "  ALT (identity)",  String.format("%,.0f µs/query", altIdUs) },
                { "  ALT (Z-order)",   String.format("%,.0f µs/query  (%.2fx speedup)", altZUs, altSpeedup) },
                { "  CH  (identity)",  String.format("%,.0f µs/query", chIdUs) },
                { "  CH  (Z-order)",   String.format("%,.0f µs/query  (%.2fx speedup)", chZUs, chSpeedup) },
                null,
                { "Correctness" },
                { "  Max cost diff",   String.format("%.6f", maxCostDiff) },
                { "  Mismatches",      String.format("%d  (threshold: 1e-3)", mismatches) },
        });
    }

    /**
     * Benchmarks a single router with the given query pairs.
     * Returns total elapsed nanoseconds for all queries.
     */
    private static long benchmarkRouter(LeastCostPathCalculator router,
                                         List<Node> nodeList, int[][] pairs, String label) {
        int total = pairs.length;
        System.out.printf("  %-20s  ", label);
        System.out.flush();
        long startNs = System.nanoTime();
        for (int i = 0; i < total; i++) {
            Node s = nodeList.get(pairs[i][0]);
            Node d = nodeList.get(pairs[i][1]);
            router.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            // progress dots every 500 queries
            if ((i + 1) % 500 == 0) {
                System.out.print(".");
                System.out.flush();
            }
        }
        long elapsedNs = System.nanoTime() - startNs;
        double avgUs = elapsedNs / (total * 1000.0);
        System.out.printf("  %,.0f µs/query%n", avgUs);
        return elapsedNs;
    }

    // -----------------------------------------------------------------------

    private static Network downloadAndLoadNetwork() {
        java.nio.file.Path localPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "berlin-v7.0-network.xml.gz");

        if (!Files.exists(localPath) || fileSize(localPath) < 1000) {
            System.out.println("Downloading Berlin v7.0 network ...");
            System.out.println("  URL:   " + NETWORK_URL);
            System.out.println("  Cache: " + localPath);
            try {
                HttpClient client = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(15))
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(NETWORK_URL))
                        .timeout(Duration.ofSeconds(120))
                        .GET()
                        .build();
                HttpResponse<InputStream> response = client.send(request,
                        HttpResponse.BodyHandlers.ofInputStream());
                if (response.statusCode() != 200) {
                    throw new RuntimeException("HTTP " + response.statusCode());
                }
                try (InputStream in = response.body()) {
                    Files.copy(in, localPath, StandardCopyOption.REPLACE_EXISTING);
                }
                System.out.printf("  Downloaded %.1f MB%n", fileSize(localPath) / (1024.0 * 1024.0));
            } catch (Exception e) {
                throw new RuntimeException("Cannot download Berlin v7.0 network: " + e.getMessage(), e);
            }
        } else {
            System.out.printf("Using cached Berlin v7.0 network: %s (%.1f MB)%n",
                    localPath, fileSize(localPath) / (1024.0 * 1024.0));
        }

        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(scenario.getNetwork()).readFile(localPath.toString());
        return scenario.getNetwork();
    }

    private static long fileSize(java.nio.file.Path path) {
        try {
            return Files.size(path);
        } catch (Exception e) {
            return 0;
        }
    }

    // -----------------------------------------------------------------------
    // Pretty-print helper: auto-sizing box with Unicode borders
    // -----------------------------------------------------------------------

    /**
     * Prints a nicely formatted box to stdout.
     *
     * @param title title line (centered)
     * @param rows  each row is either:
     *              - {@code null}              → separator line
     *              - {@code { "heading" }}     → section heading (single element)
     *              - {@code { "key", "val" }}  → key-value row
     */
    private static void printBox(String title, String[][] rows) {
        // Compute the label column width and total inner width
        int labelW = 0;
        int valueW = 0;
        for (String[] row : rows) {
            if (row == null) continue;
            labelW = Math.max(labelW, row[0].length());
            if (row.length > 1) {
                valueW = Math.max(valueW, row[1].length());
            }
        }
        int innerW = Math.max(title.length() + 4, labelW + 3 + valueW + 2);
        // make sure innerW is even enough for nice centering
        innerW = Math.max(innerW, 50);

        String hBar = "═".repeat(innerW + 2);  // +2 for padding spaces inside box

        System.out.println("╔" + hBar + "╗");
        // centered title
        int pad = (innerW + 2 - title.length()) / 2;
        System.out.println("║" + " ".repeat(pad) + title + " ".repeat(innerW + 2 - pad - title.length()) + "║");

        for (String[] row : rows) {
            if (row == null) {
                // separator
                System.out.println("╠" + "─".repeat(innerW + 2) + "╣");
            } else if (row.length == 1) {
                // section heading
                String heading = " " + row[0];
                System.out.println("║" + heading + " ".repeat(innerW + 2 - heading.length()) + "║");
            } else {
                // key : value
                String line = String.format(" %-" + labelW + "s : %s", row[0], row[1]);
                if (line.length() < innerW + 2) {
                    line = line + " ".repeat(innerW + 2 - line.length());
                }
                System.out.println("║" + line + "║");
            }
        }
        System.out.println("╚" + hBar + "╝");
    }
}
