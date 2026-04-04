package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
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
 * Standalone benchmark comparing <b>SpeedyALT</b> vs <b>CH Time-Dependent</b>
 * routing on the Berlin v7.0 network (~88k nodes).
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

    private static final int WARMUP_QUERIES    = 50;
    private static final int BENCHMARK_QUERIES = 50_000;
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

        // ---- 2. Build CH (ND-ordered) ----
        System.out.println();
        System.out.println("Building CH (InertialFlowCutter order) ...");
        SpeedyGraph graph = SpeedyGraphBuilder.build(network);

        long t0 = System.nanoTime();
        int[] order = new InertialFlowCutter(graph).computeOrder();
        long orderMs = (System.nanoTime() - t0) / 1_000_000;

        long t1 = System.nanoTime();
        SpeedyCHGraph chGraph = new SpeedyCHBuilder(graph, tc).buildWithOrder(order);
        long contractionMs = (System.nanoTime() - t1) / 1_000_000;

        new SpeedyCHTTFCustomizer().customize(chGraph, tc, tc);
        long totalBuildMs = (System.nanoTime() - t0) / 1_000_000;

        System.out.printf("  Order:       %,6d ms%n", orderMs);
        System.out.printf("  Contraction: %,6d ms%n", contractionMs);
        System.out.printf("  Total build: %,6d ms%n", totalBuildMs);
        System.out.printf("  CH edges:    %,d (base links: %,d)%n",
                chGraph.totalEdgeCount, network.getLinks().size());

        // ---- 3. Build ALT data ----
        System.out.println();
        System.out.println("Building SpeedyALT landmarks ...");
        long altBuildStart = System.nanoTime();
        SpeedyALTData altData = new SpeedyALTData(graph, Math.min(ALT_LANDMARKS, graph.nodeCount), tc, 1);
        long altBuildMs = (System.nanoTime() - altBuildStart) / 1_000_000;
        System.out.printf("  ALT build:   %,6d ms  (%d landmarks)%n", altBuildMs, ALT_LANDMARKS);

        // ---- 4. Create routers ----
        SpeedyCHTimeDep chTimeDep = new SpeedyCHTimeDep(chGraph, tc, tc);
        SpeedyALT altRouter = new SpeedyALT(altData, tc, tc);

        List<Node> nodeList = new ArrayList<>(network.getNodes().values());
        int n = nodeList.size();

        // ---- 5. Warm-up ----
        System.out.println();
        System.out.printf("Warming up (%d queries per router) ...%n", WARMUP_QUERIES);
        Random rng = new Random(42);
        for (int i = 0; i < WARMUP_QUERIES; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));
            chTimeDep.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            altRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
        }

        // ---- 6. Benchmark ----
        System.out.printf("Running benchmark (%,d queries) ...%n", BENCHMARK_QUERIES);
        rng = new Random(123);

        long altTotalNs = 0;
        long chTotalNs = 0;
        int mismatches = 0;
        double maxCostDiff = 0;

        for (int i = 0; i < BENCHMARK_QUERIES; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));

            long a0 = System.nanoTime();
            Path altPath = altRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            altTotalNs += System.nanoTime() - a0;

            long c0 = System.nanoTime();
            Path chPath = chTimeDep.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            chTotalNs += System.nanoTime() - c0;

            // quick correctness check
            if (altPath != null && chPath != null) {
                double diff = Math.abs(altPath.travelCost - chPath.travelCost);
                maxCostDiff = Math.max(maxCostDiff, diff);
                if (diff > 1e-3) {
                    mismatches++;
                    if (mismatches <= 5) {
                        System.err.printf("  MISMATCH #%d: %s->%s  ALT=%.4f  CH=%.4f  diff=%.6f%n",
                                mismatches, s.getId(), d.getId(),
                                altPath.travelCost, chPath.travelCost, diff);
                    }
                }
            }
        }

        // ---- 7. Results ----
        double altAvgUs = altTotalNs / (BENCHMARK_QUERIES * 1000.0);
        double chAvgUs  = chTotalNs  / (BENCHMARK_QUERIES * 1000.0);
        double speedup  = (double) altTotalNs / Math.max(1, chTotalNs);
        double edgeOverhead = ((double) chGraph.totalEdgeCount / network.getLinks().size() - 1) * 100;

        System.out.println();
        printBox("Berlin v7.0  —  SpeedyALT vs CH Time-Dependent", new String[][] {
                { "Network" },
                { "  Nodes",           String.format("%,d", network.getNodes().size()) },
                { "  Links",           String.format("%,d", network.getLinks().size()) },
                { "  CH edges",        String.format("%,d  (%+.1f%% overhead)", chGraph.totalEdgeCount, edgeOverhead) },
                null, // separator
                { "Preprocessing" },
                { "  CH total",        String.format("%,d ms", totalBuildMs) },
                { "    Order",         String.format("%,d ms", orderMs) },
                { "    Contraction",   String.format("%,d ms", contractionMs) },
                { "  ALT landmarks",   String.format("%,d ms  (%d landmarks)", altBuildMs, ALT_LANDMARKS) },
                null,
                { "Query Performance", String.format("(%,d queries, %d warmup)", BENCHMARK_QUERIES, WARMUP_QUERIES) },
                { "  SpeedyALT",       String.format("%,.0f µs/query", altAvgUs) },
                { "  CH TimeDep",      String.format("%,.0f µs/query", chAvgUs) },
                { "  Speedup",         String.format("%.1fx  (CH TimeDep vs SpeedyALT)", speedup) },
                null,
                { "Correctness" },
                { "  Max cost diff",   String.format("%.6f", maxCostDiff) },
                { "  Mismatches",      String.format("%d  (threshold: 1e-3)", mismatches) },
        });
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



