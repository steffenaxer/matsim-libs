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
 * Standalone benchmark comparing <b>all router variants</b> on the Berlin v7.0
 * network (~88k nodes, ~200k links):
 * <ol>
 *   <li>SpeedyALT (linked-list SpeedyGraph)</li>
 *   <li>SpeedyALT (CSR SpeedyGraph)</li>
 *   <li>CH Time-Dependent (linked-list base graph)</li>
 *   <li>CH Time-Dependent (CSR base graph)</li>
 * </ol>
 *
 * <p>Run with sufficient heap, e.g.:
 * <pre>
 *   java -Xmx8G -cp ... org.matsim.core.router.speedy.BenchmarkBerlinV70
 * </pre>
 *
 * <p>The network is downloaded from the TU Berlin SVN and cached in the
 * system temp directory.
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

        // ---- 2. Build graphs ----
        System.out.println();
        System.out.println("Building SpeedyGraph (linked-list) ...");
        SpeedyGraph graphLL = SpeedyGraphBuilder.build(network);

        System.out.println("Building SpeedyGraph (CSR) ...");
        SpeedyGraph graphCSR = SpeedyGraphBuilder.build(network);
        long csrBuildStart = System.nanoTime();
        graphCSR.buildCSR();
        long csrBuildMs = (System.nanoTime() - csrBuildStart) / 1_000_000;
        System.out.printf("  CSR build:   %,6d ms%n", csrBuildMs);

        // ---- 3. Compute ND order ONCE (topology-only, same for LL and CSR) ----
        System.out.println();
        System.out.println("Computing InertialFlowCutter ND order (shared for LL and CSR) ...");

        long t0 = System.nanoTime();
        InertialFlowCutter.NDOrderResult orderResult = new InertialFlowCutter(graphLL).computeOrderWithBatches();
        long orderMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("  Order:       %,6d ms%n", orderMs);

        // ---- 4. Build CH on linked-list graph ----
        System.out.println();
        System.out.println("Building CH on linked-list graph ...");
        long t1 = System.nanoTime();
        CHGraph chGraphLL = new CHBuilder(graphLL, tc).buildWithOrderParallel(orderResult);
        long contractionLLMs = (System.nanoTime() - t1) / 1_000_000;
        new CHTTFCustomizer().customize(chGraphLL, tc, tc);
        long totalBuildLLMs = orderMs + (System.nanoTime() - t1) / 1_000_000;

        System.out.printf("  Contraction: %,6d ms%n", contractionLLMs);
        System.out.printf("  Total (incl order): %,6d ms%n", totalBuildLLMs);
        System.out.printf("  CH edges:    %,d (base links: %,d)%n",
                chGraphLL.totalEdgeCount, network.getLinks().size());

        // ---- 5. Build CH on CSR graph (reuses same ND order) ----
        System.out.println();
        System.out.println("Building CH on CSR graph (reusing same ND order) ...");
        long t2 = System.nanoTime();
        CHGraph chGraphCSR = new CHBuilder(graphCSR, tc).buildWithOrderParallel(orderResult);
        long contractionCSRMs = (System.nanoTime() - t2) / 1_000_000;
        new CHTTFCustomizer().customize(chGraphCSR, tc, tc);
        long totalBuildCSRMs = (System.nanoTime() - t2) / 1_000_000;

        System.out.printf("  Contraction: %,6d ms%n", contractionCSRMs);
        System.out.printf("  Total (excl order): %,6d ms%n", totalBuildCSRMs);
        System.out.printf("  CH edges:    %,d%n", chGraphCSR.totalEdgeCount);

        // ---- 6. Build ALT data (LL and CSR) ----
        System.out.println();
        System.out.println("Building SpeedyALT landmarks (LL) ...");
        long altLLBuildStart = System.nanoTime();
        SpeedyALTData altDataLL = new SpeedyALTData(graphLL, Math.min(ALT_LANDMARKS, graphLL.nodeCount), tc, 1);
        long altLLBuildMs = (System.nanoTime() - altLLBuildStart) / 1_000_000;
        System.out.printf("  ALT build (LL):  %,6d ms  (%d landmarks)%n", altLLBuildMs, ALT_LANDMARKS);

        System.out.println("Building SpeedyALT landmarks (CSR) ...");
        long altCSRBuildStart = System.nanoTime();
        SpeedyALTData altDataCSR = new SpeedyALTData(graphCSR, Math.min(ALT_LANDMARKS, graphCSR.nodeCount), tc, 1);
        long altCSRBuildMs = (System.nanoTime() - altCSRBuildStart) / 1_000_000;
        System.out.printf("  ALT build (CSR): %,6d ms  (%d landmarks)%n", altCSRBuildMs, ALT_LANDMARKS);

        // ---- 7. Create all 4 routers ----
        SpeedyALT altLL       = new SpeedyALT(altDataLL, tc, tc);
        SpeedyALT altCSR      = new SpeedyALT(altDataCSR, tc, tc);
        CHRouterTimeDep chLL  = new CHRouterTimeDep(chGraphLL, tc, tc);
        CHRouterTimeDep chCSR = new CHRouterTimeDep(chGraphCSR, tc, tc);

        List<Node> nodeList = new ArrayList<>(network.getNodes().values());
        int n = nodeList.size();

        // ---- 8. Warm-up ----
        System.out.println();
        System.out.printf("Warming up (%d queries per router) ...%n", WARMUP_QUERIES);
        Random rng = new Random(42);
        for (int i = 0; i < WARMUP_QUERIES; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));
            double depTime = 8.0 * 3600;
            altLL.calcLeastCostPath(s, d, depTime, null, null);
            altCSR.calcLeastCostPath(s, d, depTime, null, null);
            chLL.calcLeastCostPath(s, d, depTime, null, null);
            chCSR.calcLeastCostPath(s, d, depTime, null, null);
        }

        // ---- 9. Benchmark ----
        System.out.printf("Running benchmark (%,d queries per router) ...%n", BENCHMARK_QUERIES);
        rng = new Random(123);

        // Pre-generate random pairs so all routers use the exact same queries
        int[][] pairs = new int[BENCHMARK_QUERIES][2];
        for (int i = 0; i < BENCHMARK_QUERIES; i++) {
            pairs[i][0] = rng.nextInt(n);
            pairs[i][1] = rng.nextInt(n);
        }

        // Benchmark each router separately to avoid interleaved GC interference
        long altLLNs   = benchmarkRouter(altLL, nodeList, pairs, "SpeedyALT (LL)");
        long altCSRNs  = benchmarkRouter(altCSR, nodeList, pairs, "SpeedyALT (CSR)");
        long chLLNs    = benchmarkRouter(chLL, nodeList, pairs, "CH TimeDep (LL)");
        long chCSRNs   = benchmarkRouter(chCSR, nodeList, pairs, "CH TimeDep (CSR)");

        // Quick correctness check: compare ALT LL vs CSR, and ALT vs CH
        int mismatches = 0;
        double maxCostDiff = 0;
        for (int i = 0; i < Math.min(200, BENCHMARK_QUERIES); i++) {
            Node s = nodeList.get(pairs[i][0]);
            Node d = nodeList.get(pairs[i][1]);
            Path pathAltLL  = altLL.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            Path pathAltCSR = altCSR.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            Path pathCH     = chLL.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            if (pathAltLL != null && pathAltCSR != null) {
                double diff = Math.abs(pathAltLL.travelCost - pathAltCSR.travelCost);
                if (diff > 1e-6) {
                    System.err.printf("  ALT CSR MISMATCH: %s->%s  LL=%.4f  CSR=%.4f%n",
                            s.getId(), d.getId(), pathAltLL.travelCost, pathAltCSR.travelCost);
                }
            }
            if (pathAltLL != null && pathCH != null) {
                double diff = Math.abs(pathAltLL.travelCost - pathCH.travelCost);
                maxCostDiff = Math.max(maxCostDiff, diff);
                if (diff > 1e-3) {
                    mismatches++;
                    if (mismatches <= 5) {
                        System.err.printf("  CH MISMATCH #%d: %s->%s  ALT=%.4f  CH=%.4f  diff=%.6f%n",
                                mismatches, s.getId(), d.getId(),
                                pathAltLL.travelCost, pathCH.travelCost, diff);
                    }
                }
            }
        }

        // ---- 10. Results ----
        double altLLAvgUs   = altLLNs   / (BENCHMARK_QUERIES * 1000.0);
        double altCSRAvgUs  = altCSRNs  / (BENCHMARK_QUERIES * 1000.0);
        double chLLAvgUs    = chLLNs    / (BENCHMARK_QUERIES * 1000.0);
        double chCSRAvgUs   = chCSRNs   / (BENCHMARK_QUERIES * 1000.0);
        double altCsrVsAltLL  = (double) altLLNs  / Math.max(1, altCSRNs);
        double chCsrVsChLL    = (double) chLLNs   / Math.max(1, chCSRNs);
        double chLLVsAltLL    = (double) altLLNs  / Math.max(1, chLLNs);
        double chCSRVsAltCSR  = (double) altCSRNs / Math.max(1, chCSRNs);
        double edgeOverhead   = ((double) chGraphLL.totalEdgeCount / network.getLinks().size() - 1) * 100;

        System.out.println();
        printBox("Berlin v7.0  —  Router Variants Comparison", new String[][] {
                { "Network" },
                { "  Nodes",           String.format("%,d", network.getNodes().size()) },
                { "  Links",           String.format("%,d", network.getLinks().size()) },
                { "  CH edges (LL)",   String.format("%,d  (%+.1f%% overhead)", chGraphLL.totalEdgeCount, edgeOverhead) },
                { "  CH edges (CSR)",  String.format("%,d", chGraphCSR.totalEdgeCount) },
                null, // separator
                { "Preprocessing" },
                { "  ND Order",        String.format("%,d ms  (shared, computed once)", orderMs) },
                { "  CH build (LL)",   String.format("%,d ms  (contraction + customize)", totalBuildLLMs) },
                { "  CH build (CSR)",  String.format("%,d ms  (contraction + customize)", totalBuildCSRMs) },
                { "  ALT build (LL)",  String.format("%,d ms  (%d landmarks)", altLLBuildMs, ALT_LANDMARKS) },
                { "  ALT build (CSR)", String.format("%,d ms  (%d landmarks)", altCSRBuildMs, ALT_LANDMARKS) },
                { "  CSR build",       String.format("%,d ms", csrBuildMs) },
                null,
                { "Query Performance", String.format("(%,d queries, %d warmup)", BENCHMARK_QUERIES, WARMUP_QUERIES) },
                { "  SpeedyALT (LL)",  String.format("%,.0f µs/query", altLLAvgUs) },
                { "  SpeedyALT (CSR)", String.format("%,.0f µs/query  (%.2fx vs ALT-LL)", altCSRAvgUs, altCsrVsAltLL) },
                { "  CH TimeDep (LL)", String.format("%,.0f µs/query", chLLAvgUs) },
                { "  CH TimeDep (CSR)",String.format("%,.0f µs/query  (%.2fx vs CH-LL)", chCSRAvgUs, chCsrVsChLL) },
                null,
                { "Speedups" },
                { "  CH(LL) vs ALT(LL)",   String.format("%.2fx", chLLVsAltLL) },
                { "  CH(CSR) vs ALT(CSR)", String.format("%.2fx", chCSRVsAltCSR) },
                { "  CSR vs LL (ALT)",     String.format("%.2fx", altCsrVsAltLL) },
                { "  CSR vs LL (CH)",      String.format("%.2fx", chCsrVsChLL) },
                null,
                { "Correctness" },
                { "  Max cost diff",   String.format("%.6f", maxCostDiff) },
                { "  Mismatches",      String.format("%d  (ALT vs CH, threshold: 1e-3)", mismatches) },
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



