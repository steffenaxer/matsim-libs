/* *********************************************************************** *
 * project: org.matsim.*
 * RouterBenchmark.java
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
package org.matsim.benchmark;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.speedy.CHBuilder;
import org.matsim.core.router.speedy.CHBuilderParams;
import org.matsim.core.router.speedy.CHGraph;
import org.matsim.core.router.speedy.CHRouterTimeDep;
import org.matsim.core.router.speedy.CHTTFCustomizer;
import org.matsim.core.router.speedy.IFCParams;
import org.matsim.core.router.speedy.InertialFlowCutter;
import org.matsim.core.router.speedy.NetworkAnalyzer;
import org.matsim.core.router.speedy.NetworkProfile;
import org.matsim.core.router.speedy.RoutingParameterTuner;
import org.matsim.core.router.speedy.SpeedyALT;
import org.matsim.core.router.speedy.SpeedyALTData;
import org.matsim.core.router.speedy.SpeedyGraph;
import org.matsim.core.router.speedy.SpeedyGraphBuilder;
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
import java.util.Locale;
import java.util.Random;

/**
 * Standalone benchmark comparing <b>router variants</b> on a MATSim network.
 *
 * <p>Supports two built-in networks that are automatically downloaded when
 * {@code --network} is not specified:
 * <ul>
 *   <li><b>berlin</b> (default) — Berlin v7.0 (~88k nodes, ~200k links)</li>
 *   <li><b>duesseldorf</b> — Düsseldorf v1.2 network</li>
 * </ul>
 *
 * <p>Routers benchmarked:
 * <ol>
 *   <li>SpeedyALT (Z-order)</li>
 *   <li>CH Time-Dependent (Z-order)</li>
 * </ol>
 *
 * <p>Run with sufficient heap, e.g.:
 * <pre>
 *   java -Xmx8G -cp ... org.matsim.benchmark.RouterBenchmark \
 *        [--network path/to/network.xml.gz | berlin | duesseldorf] \
 *        [--queries 2000] [--warmup 200] [--landmarks 16]
 * </pre>
 *
 * <p>If {@code --network} is omitted, the Berlin v7.0 network is used.
 *
 * @author Steffen Axer
 */
public class RouterBenchmark {

    private static final String BERLIN_NETWORK_URL =
            "https://svn.vsp.tu-berlin.de/repos/public-svn/matsim/scenarios/countries/de/berlin/"
                    + "berlin-v7.0/input/berlin-v7.0-network.xml.gz";

    private static final String DUESSELDORF_NETWORK_URL =
            "https://svn.vsp.tu-berlin.de/repos/public-svn/matsim/scenarios/countries/de/duesseldorf/"
                    + "duesseldorf-v1.0/input/duesseldorf-v1.2-network.xml.gz";

    private static int warmupQueries    = 200;
    private static int benchmarkQueries = 2_000;
    private static int altLandmarks     = 16;

    record RouterEntry(String name, LeastCostPathCalculator router) {}

    public static void main(String[] args) {
        Locale.setDefault(Locale.US);

        Configurator.setLevel(CHBuilder.class.getName(), Level.DEBUG);
        Configurator.setLevel(InertialFlowCutter.class.getName(), Level.DEBUG);
        Configurator.setLevel(SpeedyGraphBuilder.class.getName(), Level.DEBUG);
        Configurator.setLevel(NetworkAnalyzer.class.getName(), Level.DEBUG);
        Configurator.setLevel(RoutingParameterTuner.class.getName(), Level.DEBUG);

        String networkPath = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--network"   -> networkPath       = args[++i];
                case "--queries"   -> benchmarkQueries   = Integer.parseInt(args[++i]);
                case "--warmup"    -> warmupQueries       = Integer.parseInt(args[++i]);
                case "--landmarks" -> altLandmarks        = Integer.parseInt(args[++i]);
                default -> {
                    System.err.println("Unknown argument: " + args[i]);
                    System.err.println("Usage: java ... RouterBenchmark "
                            + "[--network <path|berlin|duesseldorf>] [--queries <n>] [--warmup <n>] [--landmarks <n>]");
                    System.exit(1);
                }
            }
        }

        long maxHeapMB = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        System.out.printf("JVM max heap: %,d MB%n", maxHeapMB);
        if (maxHeapMB < 3000) {
            System.err.println("ERROR: Need at least -Xmx4G. Current max heap: " + maxHeapMB + " MB.");
            System.err.println("  Usage: java -Xmx8G -cp <classpath> " + RouterBenchmark.class.getName());
            System.exit(1);
        }

        Network network = loadNetwork(networkPath);
        System.out.printf("Network: %,d nodes, %,d links%n",
                network.getNodes().size(), network.getLinks().size());

        FreespeedTravelTimeAndDisutility tc =
                new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

        System.out.println();
        System.out.println("Building SpeedyGraph (Morton Z-order) ...");
        SpeedyGraph graph = SpeedyGraphBuilder.buildWithSpatialOrdering(network);

        System.out.println();
        System.out.println("Analysing network structure ...");
        NetworkProfile profile = NetworkAnalyzer.analyze(graph);
        System.out.println("  " + profile.toSummaryString());

        CHBuilderParams chParams = RoutingParameterTuner.tuneCHParams(profile);
        IFCParams ifcParams = RoutingParameterTuner.tuneIFCParams(profile);
        int autoLandmarks = RoutingParameterTuner.tuneLandmarkCount(profile, 0);
        int effectiveLandmarks = altLandmarks > 0 ? altLandmarks : autoLandmarks;

        System.out.printf("  Auto-tuned CH params:  %s%n", chParams);
        System.out.printf("  Auto-tuned IFC params: %s%n", ifcParams);
        System.out.printf("  Landmarks: %d (auto=%d, cli=%d)%n", effectiveLandmarks, autoLandmarks, altLandmarks);

        System.out.println();
        System.out.println("Computing InertialFlowCutter ND order (auto-tuned) ...");
        long t0 = System.nanoTime();
        InertialFlowCutter.NDOrderResult order = new InertialFlowCutter(graph, ifcParams).computeOrderWithBatches();
        long orderMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("  ND Order:  %,6d ms%n", orderMs);

        System.out.println();
        System.out.println("Building CH (auto-tuned) ...");
        long t1 = System.nanoTime();
        CHGraph chGraph = new CHBuilder(graph, tc, chParams).buildWithOrderParallel(order);
        new CHTTFCustomizer().customize(chGraph, tc, tc);
        long totalBuildMs = orderMs + (System.nanoTime() - t1) / 1_000_000;
        System.out.printf("  CH build:  %,6d ms  (%,d edges)%n", totalBuildMs, chGraph.getTotalEdgeCount());

        System.out.println();
        System.out.println("Building SpeedyALT landmarks ...");
        long altStart = System.nanoTime();
        SpeedyALTData altData = new SpeedyALTData(graph, Math.min(effectiveLandmarks, graph.getNodeCount()), tc, 1);
        long altMs = (System.nanoTime() - altStart) / 1_000_000;
        System.out.printf("  ALT build: %,6d ms  (%d landmarks)%n", altMs, effectiveLandmarks);

        List<RouterEntry> routers = buildRouters(altData, chGraph, tc);

        List<Node> nodeList = new ArrayList<>(network.getNodes().values());
        int n = nodeList.size();

        System.out.println();
        System.out.printf("Warming up (%d queries per router) ...%n", warmupQueries);
        Random rng = new Random(42);
        for (int i = 0; i < warmupQueries; i++) {
            Node s = nodeList.get(rng.nextInt(n));
            Node d = nodeList.get(rng.nextInt(n));
            double depTime = 8.0 * 3600;
            for (RouterEntry entry : routers) {
                entry.router().calcLeastCostPath(s, d, depTime, null, null);
            }
        }

        System.out.printf("Running benchmark (%,d queries per router) ...%n", benchmarkQueries);
        rng = new Random(123);
        int[][] pairs = new int[benchmarkQueries][2];
        for (int i = 0; i < benchmarkQueries; i++) {
            pairs[i][0] = rng.nextInt(n);
            pairs[i][1] = rng.nextInt(n);
        }

        long[] elapsedNs = new long[routers.size()];
        for (int r = 0; r < routers.size(); r++) {
            elapsedNs[r] = benchmarkRouter(routers.get(r).router(), nodeList, pairs, routers.get(r).name());
        }

        LeastCostPathCalculator refRouter = routers.getFirst().router();
        int mismatches = 0;
        double maxCostDiff = 0;
        for (int i = 0; i < Math.min(200, benchmarkQueries); i++) {
            Node s = nodeList.get(pairs[i][0]);
            Node d = nodeList.get(pairs[i][1]);
            Path refPath = refRouter.calcLeastCostPath(s, d, 8.0 * 3600, null, null);
            for (int r = 1; r < routers.size(); r++) {
                Path otherPath = routers.get(r).router().calcLeastCostPath(s, d, 8.0 * 3600, null, null);
                if (refPath != null && otherPath != null) {
                    double diff = Math.abs(refPath.travelCost - otherPath.travelCost);
                    maxCostDiff = Math.max(maxCostDiff, diff);
                    if (diff > 1e-3) {
                        mismatches++;
                        if (mismatches <= 5) {
                            System.err.printf("  MISMATCH #%d: %s->%s  %s=%.4f  %s=%.4f  diff=%.6f%n",
                                    mismatches, s.getId(), d.getId(),
                                    routers.getFirst().name(), refPath.travelCost,
                                    routers.get(r).name(), otherPath.travelCost, diff);
                        }
                    }
                }
            }
        }

        double edgeOverhead = ((double) chGraph.getTotalEdgeCount() / network.getLinks().size() - 1) * 100;

        int nodeCount = network.getNodes().size();
        List<String[]> resultRows = new ArrayList<>();
        resultRows.add(new String[]{ "Network" });
        resultRows.add(new String[]{ "  Nodes",           String.format("%,d", nodeCount) });
        resultRows.add(new String[]{ "  Links",           String.format("%,d", network.getLinks().size()) });
        resultRows.add(new String[]{ "  CH edges",        String.format("%,d  (%+.1f%% overhead)", chGraph.getTotalEdgeCount(), edgeOverhead) });
        resultRows.add(new String[]{ "  Param mode",      "auto-tuned" });
        resultRows.add(new String[]{ "  Avg out-degree",  String.format("%.2f", profile.avgOutDegree()) });
        resultRows.add(new String[]{ "  P95 out-degree",  String.valueOf(profile.p95OutDegree()) });
        resultRows.add(new String[]{ "  Max out-degree",  String.valueOf(profile.maxOutDegree()) });
        resultRows.add(new String[]{ "  Hub fraction",    String.format("%.4f (deg>=6)", profile.highDegreeNodeFraction()) });
        resultRows.add(new String[]{ "  Deg skewness",    String.format("%.2f", profile.degreeSkewness()) });
        resultRows.add(new String[]{ "  Est. diameter",   String.valueOf(profile.estimatedDiameter()) });
        resultRows.add(new String[]{ "  Components",      String.valueOf(profile.connectedComponents()) });
        resultRows.add(null);
        resultRows.add(new String[]{ "Preprocessing" });
        resultRows.add(new String[]{ "  ND Order",        String.format("%,d ms", orderMs) });
        resultRows.add(new String[]{ "  CH build",        String.format("%,d ms  (incl. order)", totalBuildMs) });
        resultRows.add(new String[]{ "  ALT build",       String.format("%,d ms  (%d landmarks)", altMs, effectiveLandmarks) });
        resultRows.add(null);
        resultRows.add(new String[]{ "Query Performance", String.format("(%,d queries, %d warmup)", benchmarkQueries, warmupQueries) });
        for (int r = 0; r < routers.size(); r++) {
            double avgUs = elapsedNs[r] / (benchmarkQueries * 1000.0);
            resultRows.add(new String[]{ "  " + routers.get(r).name(), String.format("%,.0f µs/query", avgUs) });
        }
        resultRows.add(null);
        resultRows.add(new String[]{ "Correctness" });
        resultRows.add(new String[]{ "  Max cost diff",   String.format("%.6f", maxCostDiff) });
        resultRows.add(new String[]{ "  Mismatches",      String.format("%d  (threshold: 1e-3)", mismatches) });

        System.out.println();
        printBox("Routing Benchmark  —  ALT vs CH Comparison", resultRows.toArray(String[][]::new));
    }

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

    private static Network loadNetwork(String networkPath) {
        if (networkPath == null || "berlin".equalsIgnoreCase(networkPath)) {
            return downloadAndLoadNetwork(BERLIN_NETWORK_URL, "berlin-v7.0-network.xml.gz", "Berlin v7.0");
        }
        if ("duesseldorf".equalsIgnoreCase(networkPath)) {
            return downloadAndLoadNetwork(DUESSELDORF_NETWORK_URL,
                    "duesseldorf-v1.2-network.xml.gz", "Düsseldorf v1.2");
        }
        System.out.println("Loading network from " + networkPath + " ...");
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(scenario.getNetwork()).readFile(networkPath);
        return scenario.getNetwork();
    }

    private static List<RouterEntry> buildRouters(
            SpeedyALTData altData,
            CHGraph chGraph,
            FreespeedTravelTimeAndDisutility tc) {
        List<RouterEntry> routers = new ArrayList<>();
        routers.add(new RouterEntry("SpeedyALT",     new SpeedyALT(altData, tc, tc)));
        routers.add(new RouterEntry("CH (time-dep)",  new CHRouterTimeDep(chGraph, tc, tc)));
        return routers;
    }

    private static Network downloadAndLoadNetwork(String url, String filename, String label) {
        java.nio.file.Path localPath = Paths.get(System.getProperty("java.io.tmpdir"), filename);

        if (!Files.exists(localPath) || fileSize(localPath) < 1000) {
            System.out.println("Downloading " + label + " network ...");
            System.out.println("  URL:   " + url);
            System.out.println("  Cache: " + localPath);
            try {
                HttpClient client = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(15))
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(300))
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
                throw new RuntimeException("Cannot download " + label + " network: " + e.getMessage(), e);
            }
        } else {
            System.out.printf("Using cached %s network: %s (%.1f MB)%n",
                    label, localPath, fileSize(localPath) / (1024.0 * 1024.0));
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

    private static void printBox(String title, String[][] rows) {
        int labelW = 0;
        int valueW = 0;
        for (String[] row : rows) {
            if (row == null) continue;
            labelW = Math.max(labelW, row[0].length());
            if (row.length > 1) valueW = Math.max(valueW, row[1].length());
        }
        int innerW = Math.max(Math.max(title.length() + 4, labelW + 3 + valueW + 2), 50);
        String hBar = "═".repeat(innerW + 2);
        System.out.println("╔" + hBar + "╗");
        int pad = (innerW + 2 - title.length()) / 2;
        System.out.println("║" + " ".repeat(pad) + title + " ".repeat(innerW + 2 - pad - title.length()) + "║");
        for (String[] row : rows) {
            if (row == null) {
                System.out.println("╠" + "─".repeat(innerW + 2) + "╣");
            } else if (row.length == 1) {
                String heading = " " + row[0];
                System.out.println("║" + heading + " ".repeat(innerW + 2 - heading.length()) + "║");
            } else {
                String line = String.format(" %-" + labelW + "s : %s", row[0], row[1]);
                if (line.length() < innerW + 2) line = line + " ".repeat(innerW + 2 - line.length());
                System.out.println("║" + line + "║");
            }
        }
        System.out.println("╚" + hBar + "╝");
    }
}
