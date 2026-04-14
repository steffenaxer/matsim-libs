/* *********************************************************************** *
 * project: org.matsim.*
 * CHQueryExporter.java
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.utils.geometry.CoordinateTransformation;
import org.matsim.core.utils.geometry.transformations.TransformationFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Standalone tool that runs an instrumented CH query and SpeedyALT query on the
 * Berlin network and exports the step-by-step search state as JSON for the
 * {@code ch-visualization.html} interactive visualization.
 *
 * <h3>Output format ({@code ch-query-data.json})</h3>
 * <pre>
 * {
 *   "odPairs": [ { "name": "Kreuzberg→Charlottenburg", "startNode": 42, "endNode": 7 }, ... ],
 *   "nodes": [ { "id": 0, "lon": 13.38, "lat": 52.52, "level": 150 }, ... ],
 *   "edges": [ { "from": 0, "to": 1 }, ... ],
 *   "queries": [
 *     {
 *       "odIndex": 0,
 *       "startNode": 42, "endNode": 7,
 *       "chEvents":      [ { "step":0, "dir":"fwd", "node":42, "cost":0.0, "parent":-1, "stalled":false, "meeting":false }, ... ],
 *       "speedyALTEvents":[ { "step":0, "node":42, "cost":0.0, "parent":-1 }, ... ],
 *       "path":          [ nodeIdx, ... ],
 *       "pathLinks":     [ { "from": nodeIdx, "to": nodeIdx }, ... ],
 *       "routeCost":     1234.5678,
 *       "routeLinks":    42,
 *       "shortcutTree":  { "gIdx":5, "isShortcut":true, "children":[...] }
 *     }, ...
 *   ]
 * }
 * </pre>
 *
 * <h3>Usage</h3>
 * <pre>
 *   java CHQueryExporter [networkFile] [outputFile]
 *
 *   Defaults:
 *     networkFile = examples/scenarios/berlin/network.xml.gz
 *     outputFile  = matsim/tools/ch-viz/ch-query-data.json
 * </pre>
 *
 * @author Steffen Axer
 */
public class CHQueryExporter {

    private static final Logger LOG = LogManager.getLogger(CHQueryExporter.class);

    // OD pairs defined as GK4 coordinates [startX, startY, endX, endY]
    // Long-distance pairs across the full Berlin network for impressive visualizations.
    private static final double[][] OD_COORDS_GK4 = {
        // Spandau (west) → Marzahn-Hellersdorf (east) — full cross-city
        {4576000, 5822000, 4615000, 5821000},
        // Frohnau (north) → Lichtenrade (south) — full north-south traverse
        {4596000, 5837000, 4600000, 5803000},
        // Wannsee (southwest) → Karow (northeast) — diagonal across Berlin
        {4573000, 5810000, 4609000, 5837000},
        // Tegel (northwest) → Adlershof (southeast) — long diagonal
        {4582000, 5831000, 4610000, 5808000},
    };

    private static final String[] OD_NAMES = {
        "Spandau → Marzahn (West-East)",
        "Frohnau → Lichtenrade (North-South)",
        "Wannsee → Karow (SW-NE Diagonal)",
        "Tegel → Adlershof (NW-SE Diagonal)",
    };

    public static void main(String[] args) throws Exception {
        String networkFile = "examples/scenarios/berlin/network.xml.gz";
        String outputFile  = "matsim/tools/ch-viz/ch-query-data.json";

        if (args.length >= 1) networkFile = args[0];
        if (args.length >= 2) outputFile  = args[1];

        LOG.info("Loading network: {}", networkFile);
        Network network = NetworkUtils.readNetwork(networkFile);
        LOG.info("Network loaded: {} nodes, {} links", network.getNodes().size(), network.getLinks().size());

        LOG.info("Building SpeedyGraph (identity ordering)...");
        SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);

        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());

        LOG.info("Building SpeedyALTData (16 landmarks)...");
        SpeedyALTData altData = new SpeedyALTData(baseGraph, Math.min(16, baseGraph.nodeCount), tc, 4);

        LOG.info("Building CHGraph (this may take a minute for the Berlin network)...");
        CHGraph chGraph = new CHBuilder(baseGraph, tc).build();
        new CHCustomizer().customize(chGraph, tc);
        LOG.info("CHGraph built: {} nodes, {} edges (up={}, dn={})",
                chGraph.nodeCount, chGraph.totalEdgeCount, chGraph.upEdgeCount, chGraph.dnEdgeCount);

        // Coordinate transformation GK4 → WGS84
        CoordinateTransformation transform = TransformationFactory.getCoordinateTransformation(
                TransformationFactory.GK4, TransformationFactory.WGS84);

        // Resolve OD pairs to node indices
        int[][] odNodePairs = resolveODPairs(baseGraph, network, transform);

        LOG.info("Exporting query data to: {}", outputFile);
        exportJSON(baseGraph, chGraph, altData, transform, odNodePairs, outputFile);
        LOG.info("Done. Output: {}", outputFile);
    }

    // -----------------------------------------------------------------------
    // OD pair resolution
    // -----------------------------------------------------------------------

    private static int[][] resolveODPairs(SpeedyGraph graph, Network network,
                                           CoordinateTransformation transform) {
        int[][] pairs = new int[OD_COORDS_GK4.length][2];
        for (int i = 0; i < OD_COORDS_GK4.length; i++) {
            double sx = OD_COORDS_GK4[i][0], sy = OD_COORDS_GK4[i][1];
            double ex = OD_COORDS_GK4[i][2], ey = OD_COORDS_GK4[i][3];
            pairs[i][0] = findNearestNode(graph, network, sx, sy);
            pairs[i][1] = findNearestNode(graph, network, ex, ey);
            LOG.info("OD pair {}: node {} → node {} ({})",
                    i, pairs[i][0], pairs[i][1], OD_NAMES[i]);
        }
        return pairs;
    }

    private static int findNearestNode(SpeedyGraph graph, Network network,
                                        double targetX, double targetY) {
        double minDist2 = Double.MAX_VALUE;
        int bestIdx = 0;
        for (Node node : network.getNodes().values()) {
            int idx = graph.getNodeIndex(node);
            if (idx < 0) continue;
            double dx = node.getCoord().getX() - targetX;
            double dy = node.getCoord().getY() - targetY;
            double d2 = dx * dx + dy * dy;
            if (d2 < minDist2) {
                minDist2 = d2;
                bestIdx  = idx;
            }
        }
        return bestIdx;
    }

    // -----------------------------------------------------------------------
    // JSON export
    // -----------------------------------------------------------------------

    private static void exportJSON(SpeedyGraph graph, CHGraph chGraph, SpeedyALTData altData,
                                    CoordinateTransformation transform,
                                    int[][] odPairs, String outputFile) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(outputFile))) {
            pw.println("{");

            // --- OD pair metadata ---
            pw.println("\"odPairs\": [");
            for (int i = 0; i < odPairs.length; i++) {
                pw.printf(Locale.US, "  {\"name\":\"%s\",\"startNode\":%d,\"endNode\":%d}%s%n",
                        escapeJson(OD_NAMES[i]), odPairs[i][0], odPairs[i][1],
                        i < odPairs.length - 1 ? "," : "");
            }
            pw.println("],");

            // --- Nodes ---
            int nodeCount = graph.nodeCount;
            pw.println("\"nodes\": [");
            for (int i = 0; i < nodeCount; i++) {
                Node node = graph.getNode(i);
                Coord wgs84 = transform.transform(node.getCoord());
                int level = chGraph.nodeLevel[i];
                pw.printf(Locale.US, "  {\"id\":%d,\"lon\":%.6f,\"lat\":%.6f,\"level\":%d}%s%n",
                        i, wgs84.getX(), wgs84.getY(), level,
                        i < nodeCount - 1 ? "," : "");
            }
            pw.println("],");

            // --- Edges (base network links only) ---
            pw.println("\"edges\": [");
            int linkCount = graph.linkCount;
            boolean firstEdge = true;
            for (int li = 0; li < linkCount; li++) {
                Link link = graph.getLink(li);
                if (link == null) continue;
                int fromIdx = graph.getNodeIndex(link.getFromNode());
                int toIdx   = graph.getNodeIndex(link.getToNode());
                if (fromIdx < 0 || toIdx < 0) continue;
                if (!firstEdge) pw.print(",");
                pw.printf(Locale.US, "%n  {\"from\":%d,\"to\":%d}", fromIdx, toIdx);
                firstEdge = false;
            }
            pw.println("\n],");

            // --- Per-query data ---
            pw.println("\"queries\": [");
            for (int qi = 0; qi < odPairs.length; qi++) {
                int startIdx = odPairs[qi][0];
                int endIdx   = odPairs[qi][1];
                pw.printf("  {\"odIndex\":%d,\"startNode\":%d,\"endNode\":%d,%n",
                        qi, startIdx, endIdx);

                // CH query
                CHQueryResult chResult = runInstrumentedCHQuery(chGraph, startIdx, endIdx);
                pw.println("  \"chEvents\": [");
                for (int ei = 0; ei < chResult.events.size(); ei++) {
                    CHEvent e = chResult.events.get(ei);
                    pw.printf(Locale.US,
                            "    {\"step\":%d,\"dir\":\"%s\",\"node\":%d,\"cost\":%.4f,\"parent\":%d,\"stalled\":%b,\"meeting\":%b}%s%n",
                            e.step(), e.dir(), e.node(), e.cost(), e.parent(), e.stalled(), e.meeting(),
                            ei < chResult.events.size() - 1 ? "," : "");
                }
                pw.println("  ],");

                // SpeedyALT query
                SpeedyALTQueryResult altResult = runInstrumentedSpeedyALT(graph, altData, startIdx, endIdx);
                pw.println("  \"speedyALTEvents\": [");
                for (int ei = 0; ei < altResult.events.size(); ei++) {
                    SettleEvent e = altResult.events.get(ei);
                    pw.printf(Locale.US, "    {\"step\":%d,\"node\":%d,\"cost\":%.4f,\"parent\":%d}%s%n",
                            e.step(), e.node(), e.cost(), e.parent(),
                            ei < altResult.events.size() - 1 ? "," : "");
                }
                pw.println("  ],");

                // Path nodes
                pw.print("  \"path\": [");
                List<Integer> pathNodes = chResult.pathNodes;
                for (int pi = 0; pi < pathNodes.size(); pi++) {
                    if (pi > 0) pw.print(",");
                    pw.print(pathNodes.get(pi));
                }
                pw.println("],");

                // Path edges
                pw.println("  \"pathLinks\": [");
                List<int[]> pathLinks = chResult.pathLinks;
                for (int pi = 0; pi < pathLinks.size(); pi++) {
                    pw.printf("    {\"from\":%d,\"to\":%d}%s%n",
                            pathLinks.get(pi)[0], pathLinks.get(pi)[1],
                            pi < pathLinks.size() - 1 ? "," : "");
                }
                pw.println("  ],");

                // Route cost and link count
                pw.printf(Locale.US, "  \"routeCost\": %.4f,%n", chResult.routeCost);
                pw.printf("  \"routeLinks\": %d,%n", chResult.pathLinks.size());

                // Shortcut tree (for first CH edge only if available)
                pw.print("  \"shortcutTree\": ");
                if (chResult.rootEdgeGIdx >= 0) {
                    writeShortcutTree(pw, chGraph, chResult.rootEdgeGIdx);
                } else {
                    pw.print("null");
                }
                pw.println();

                pw.printf("  }%s%n", qi < odPairs.length - 1 ? "," : "");
            }
            pw.println("]");
            pw.println("}");
        }
    }

    // -----------------------------------------------------------------------
    // Instrumented CH query (based on CHRouter.calcLeastCostPathImpl)
    // -----------------------------------------------------------------------

    private static CHQueryResult runInstrumentedCHQuery(CHGraph chGraph, int startIdx, int endIdx) {
        int n = chGraph.nodeCount;
        final int S = CHGraph.E_STRIDE;

        double[] fwdCost       = new double[n];
        int[]    fwdComingFrom = new int[n];
        int[]    fwdUsedEdge   = new int[n];
        int[]    fwdIterIds    = new int[n];

        double[] bwdCost       = new double[n];
        int[]    bwdComingFrom = new int[n];
        int[]    bwdUsedEdge   = new int[n];
        int[]    bwdIterIds    = new int[n];

        int iteration = 1;
        Arrays.fill(fwdIterIds, 0);
        Arrays.fill(bwdIterIds, 0);

        DAryMinHeap fwdPQ = new DAryMinHeap(n, 6);
        DAryMinHeap bwdPQ = new DAryMinHeap(n, 6);

        int[]    upOff = chGraph.upOff, upLen = chGraph.upLen, upEdges = chGraph.upEdges;
        double[] upWeights = chGraph.upWeights;
        int[]    dnOff = chGraph.dnOff, dnLen = chGraph.dnLen, dnEdges = chGraph.dnEdges;
        double[] dnWeights = chGraph.dnWeights;
        int[]    nodeLevel = chGraph.nodeLevel;

        // Initialize forward
        fwdCost[startIdx]       = 0.0;
        fwdComingFrom[startIdx] = -1;
        fwdUsedEdge[startIdx]   = -1;
        fwdIterIds[startIdx]    = iteration;
        fwdPQ.insert(startIdx, 0.0);

        // Initialize backward
        bwdCost[endIdx]       = 0.0;
        bwdComingFrom[endIdx] = -1;
        bwdUsedEdge[endIdx]   = -1;
        bwdIterIds[endIdx]    = iteration;
        bwdPQ.insert(endIdx, 0.0);

        List<CHEvent> events = new ArrayList<>();
        int step = 0;
        double bestCost   = Double.POSITIVE_INFINITY;
        int    meetingNode = -1;

        while (!fwdPQ.isEmpty() || !bwdPQ.isEmpty()) {
            double fMin = fwdPQ.isEmpty() ? Double.POSITIVE_INFINITY : fwdCost[fwdPQ.peek()];
            double bMin = bwdPQ.isEmpty() ? Double.POSITIVE_INFINITY : bwdCost[bwdPQ.peek()];
            if (fMin >= bestCost && bMin >= bestCost) break;

            boolean expandForward = !fwdPQ.isEmpty() && (bwdPQ.isEmpty() || fMin <= bMin);

            if (expandForward) {
                int v    = fwdPQ.poll();
                double d = fwdCost[v];

                boolean isMeeting = false;
                if (bwdIterIds[v] == iteration) {
                    double total = d + bwdCost[v];
                    if (total < bestCost) {
                        bestCost    = total;
                        meetingNode = v;
                        isMeeting   = true;
                    }
                }

                boolean stalled = false;
                {
                    int dOff = dnOff[v], dEnd = dOff + dnLen[v];
                    for (int slot = dOff; slot < dEnd; slot++) {
                        int u = dnEdges[slot * S];
                        if (fwdIterIds[u] == iteration) {
                            if (fwdCost[u] + dnWeights[slot] < d) {
                                stalled = true;
                                break;
                            }
                        }
                    }
                }

                events.add(new CHEvent(step++, "fwd", v, d, fwdComingFrom[v], stalled, isMeeting));

                if (!stalled) {
                    int uOff = upOff[v], uEnd = uOff + upLen[v];
                    for (int slot = uOff; slot < uEnd; slot++) {
                        int w = upEdges[slot * S];
                        double newCost = d + upWeights[slot];
                        if (fwdIterIds[w] == iteration) {
                            if (newCost < fwdCost[w]) {
                                fwdCost[w]       = newCost;
                                fwdComingFrom[w] = v;
                                fwdUsedEdge[w]   = upEdges[slot * S + CHGraph.E_GIDX];
                                fwdPQ.decreaseKey(w, newCost);
                            } else if (newCost == fwdCost[w]) {
                                int gIdx = upEdges[slot * S + CHGraph.E_GIDX];
                                if (gIdx < fwdUsedEdge[w]) {
                                    fwdComingFrom[w] = v;
                                    fwdUsedEdge[w]   = gIdx;
                                }
                            }
                        } else {
                            fwdCost[w]       = newCost;
                            fwdComingFrom[w] = v;
                            fwdUsedEdge[w]   = upEdges[slot * S + CHGraph.E_GIDX];
                            fwdIterIds[w]    = iteration;
                            fwdPQ.insert(w, newCost);
                        }
                    }
                }
            } else {
                int v    = bwdPQ.poll();
                double d = bwdCost[v];

                boolean isMeeting = false;
                if (fwdIterIds[v] == iteration) {
                    double total = fwdCost[v] + d;
                    if (total < bestCost) {
                        bestCost    = total;
                        meetingNode = v;
                        isMeeting   = true;
                    }
                }

                boolean stalled = false;
                {
                    int uOff = upOff[v], uEnd = uOff + upLen[v];
                    for (int slot = uOff; slot < uEnd; slot++) {
                        int w = upEdges[slot * S];
                        if (bwdIterIds[w] == iteration) {
                            if (bwdCost[w] + upWeights[slot] < d) {
                                stalled = true;
                                break;
                            }
                        }
                    }
                }

                events.add(new CHEvent(step++, "bwd", v, d, bwdComingFrom[v], stalled, isMeeting));

                if (!stalled) {
                    int dOff = dnOff[v], dEnd = dOff + dnLen[v];
                    for (int slot = dOff; slot < dEnd; slot++) {
                        int y = dnEdges[slot * S];
                        double newCost = d + dnWeights[slot];
                        if (bwdIterIds[y] == iteration) {
                            if (newCost < bwdCost[y]) {
                                bwdCost[y]       = newCost;
                                bwdComingFrom[y] = v;
                                bwdUsedEdge[y]   = dnEdges[slot * S + CHGraph.E_GIDX];
                                bwdPQ.decreaseKey(y, newCost);
                            } else if (newCost == bwdCost[y]) {
                                int gIdx = dnEdges[slot * S + CHGraph.E_GIDX];
                                if (gIdx < bwdUsedEdge[y]) {
                                    bwdComingFrom[y] = v;
                                    bwdUsedEdge[y]   = gIdx;
                                }
                            }
                        } else {
                            bwdCost[y]       = newCost;
                            bwdComingFrom[y] = v;
                            bwdUsedEdge[y]   = dnEdges[slot * S + CHGraph.E_GIDX];
                            bwdIterIds[y]    = iteration;
                            bwdPQ.insert(y, newCost);
                        }
                    }
                }
            }
        }

        LOG.info("CH query {}->{}: {} events, meeting={}, bestCost={}", startIdx, endIdx,
                events.size(), meetingNode, bestCost);

        // Reconstruct path
        List<Integer> pathNodes = new ArrayList<>();
        List<int[]>   pathLinks = new ArrayList<>();
        int rootEdgeGIdx = -1;

        if (meetingNode >= 0) {
            // Forward path: startIdx → meetingNode
            List<Integer> fwdGEdges = new ArrayList<>();
            int curr = meetingNode;
            while (fwdComingFrom[curr] >= 0) {
                fwdGEdges.add(fwdUsedEdge[curr]);
                curr = fwdComingFrom[curr];
            }
            Collections.reverse(fwdGEdges);

            pathNodes.add(startIdx);
            SpeedyGraph baseGraph = chGraph.getBaseGraph();
            for (int gIdx : fwdGEdges) {
                if (rootEdgeGIdx < 0) rootEdgeGIdx = gIdx;
                unpackEdgeToPath(chGraph, baseGraph, gIdx, pathNodes, pathLinks);
            }

            // Backward path: meetingNode → endIdx
            curr = meetingNode;
            while (bwdComingFrom[curr] >= 0) {
                if (rootEdgeGIdx < 0) rootEdgeGIdx = bwdUsedEdge[curr];
                unpackEdgeToPath(chGraph, baseGraph, bwdUsedEdge[curr], pathNodes, pathLinks);
                curr = bwdComingFrom[curr];
            }
        }

        return new CHQueryResult(events, pathNodes, pathLinks, rootEdgeGIdx, bestCost);
    }

    private static void unpackEdgeToPath(CHGraph chGraph, SpeedyGraph baseGraph,
                                          int gIdx, List<Integer> pathNodes, List<int[]> pathLinks) {
        int[] stack = new int[64];
        int sp = 0;
        stack[sp++] = gIdx;
        while (sp > 0) {
            int e    = stack[--sp];
            int orig = chGraph.edgeOrigLink[e];
            if (orig >= 0) {
                Link link  = baseGraph.getLink(orig);
                int toIdx  = baseGraph.getNodeIndex(link.getToNode());
                int frmIdx = pathNodes.isEmpty() ? -1 : pathNodes.get(pathNodes.size() - 1);
                pathLinks.add(new int[]{frmIdx < 0 ? toIdx : frmIdx, toIdx});
                pathNodes.add(toIdx);
            } else {
                if (sp + 2 > stack.length) stack = Arrays.copyOf(stack, stack.length * 2);
                stack[sp++] = chGraph.edgeLower2[e];
                stack[sp++] = chGraph.edgeLower1[e];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Instrumented SpeedyALT query (based on SpeedyALT.calcLeastCostPathImpl)
    // -----------------------------------------------------------------------

    private static SpeedyALTQueryResult runInstrumentedSpeedyALT(SpeedyGraph graph,
                                                                   SpeedyALTData altData,
                                                                   int startIdx, int endIdx) {
        int n = graph.nodeCount;
        double[] cost       = new double[n];
        int[]    iterIds    = new int[n];
        int[]    comingFrom = new int[n];
        Arrays.fill(iterIds, 0);

        int iteration = 1;
        DAryMinHeap pq = new DAryMinHeap(n, 6);

        int startDeadend = altData.getNodeDeadend(startIdx);
        int endDeadend   = altData.getNodeDeadend(endIdx);

        double estimation = estimateMinCost(altData, startIdx, endIdx);

        cost[startIdx]       = 0.0;
        comingFrom[startIdx] = -1;
        iterIds[startIdx]    = iteration;
        pq.insert(startIdx, 0.0 + estimation);

        List<SettleEvent> events = new ArrayList<>();
        int step = 0;
        SpeedyGraph.LinkIterator outLI = graph.getOutLinkIterator();

        while (!pq.isEmpty()) {
            int    nodeIdx  = pq.poll();
            double currCost = cost[nodeIdx];

            events.add(new SettleEvent(step++, nodeIdx, currCost, comingFrom[nodeIdx]));

            if (nodeIdx == endIdx) break;

            // Dead-end pruning (same as SpeedyALT)
            int deadend = altData.getNodeDeadend(nodeIdx);
            if (deadend >= 0 && deadend != startDeadend && deadend != endDeadend) {
                continue;
            }

            outLI.reset(nodeIdx);
            while (outLI.next()) {
                int    toNode     = outLI.getToNodeIndex();
                double travelTime = outLI.getFreespeedTravelTime();
                double newCost    = currCost + travelTime;

                if (iterIds[toNode] == iteration) {
                    if (newCost < cost[toNode]) {
                        cost[toNode]       = newCost;
                        comingFrom[toNode] = nodeIdx;
                        estimation = estimateMinCost(altData, toNode, endIdx);
                        pq.decreaseKey(toNode, newCost + estimation);
                    }
                } else {
                    cost[toNode]       = newCost;
                    comingFrom[toNode] = nodeIdx;
                    iterIds[toNode]    = iteration;
                    estimation = estimateMinCost(altData, toNode, endIdx);
                    pq.insert(toNode, newCost + estimation);
                }
            }
        }

        LOG.info("SpeedyALT query {}->{}: {} events settled", startIdx, endIdx, events.size());
        return new SpeedyALTQueryResult(events);
    }

    /**
     * ALT lower-bound estimation: max over all landmarks L of (SL−TL) and (LT−LS),
     * where S=source, T=target.
     * <ul>
     *   <li>sl = cost(source → landmark L)</li>
     *   <li>ls = cost(landmark L → source)</li>
     *   <li>tl = cost(target → landmark L)</li>
     *   <li>lt = cost(landmark L → target)</li>
     * </ul>
     * By the triangle inequality: ST ≥ SL−TL and ST ≥ LT−LS.
     */
    private static double estimateMinCost(SpeedyALTData altData, int nodeIdx, int destinationIdx) {
        double best = 0;
        for (int i = 0, nL = altData.getLandmarksCount(); i < nL; i++) {
            double sl = altData.getTravelCostToLandmark(nodeIdx, i);       // source → L
            double ls = altData.getTravelCostFromLandmark(nodeIdx, i);     // L → source
            double tl = altData.getTravelCostToLandmark(destinationIdx, i); // target → L
            double lt = altData.getTravelCostFromLandmark(destinationIdx, i); // L → target
            double estimate = Math.max(sl - tl, lt - ls);
            if (estimate > best) best = estimate;
        }
        return best;
    }

    // -----------------------------------------------------------------------
    // Shortcut unpack tree JSON writer
    // -----------------------------------------------------------------------

    private static void writeShortcutTree(PrintWriter pw, CHGraph chGraph, int gIdx) {
        writeShortcutNode(pw, chGraph, gIdx, 0);
    }

    private static void writeShortcutNode(PrintWriter pw, CHGraph chGraph, int gIdx, int depth) {
        int orig = chGraph.edgeOrigLink[gIdx];
        boolean isShortcut = (orig < 0);
        pw.printf("{\"gIdx\":%d,\"isShortcut\":%b", gIdx, isShortcut);
        if (isShortcut && depth < 20) {
            // Expand children (bounded depth to avoid huge JSON for deep chains)
            pw.print(",\"children\":[");
            writeShortcutNode(pw, chGraph, chGraph.edgeLower1[gIdx], depth + 1);
            pw.print(",");
            writeShortcutNode(pw, chGraph, chGraph.edgeLower2[gIdx], depth + 1);
            pw.print("]");
        }
        pw.print("}");
    }

    // -----------------------------------------------------------------------
    // Event and result records
    // -----------------------------------------------------------------------

    private record CHEvent(int step, String dir, int node, double cost, int parent,
                            boolean stalled, boolean meeting) {}

    private record SettleEvent(int step, int node, double cost, int parent) {}

    private static class CHQueryResult {
        final List<CHEvent>   events;
        final List<Integer>   pathNodes;
        final List<int[]>     pathLinks;
        final int             rootEdgeGIdx;
        final double          routeCost;

        CHQueryResult(List<CHEvent> events, List<Integer> pathNodes,
                      List<int[]> pathLinks, int rootEdgeGIdx, double routeCost) {
            this.events       = events;
            this.pathNodes    = pathNodes;
            this.pathLinks    = pathLinks;
            this.rootEdgeGIdx = rootEdgeGIdx;
            this.routeCost    = routeCost;
        }
    }

    private static class SpeedyALTQueryResult {
        final List<SettleEvent> events;
        SpeedyALTQueryResult(List<SettleEvent> events) { this.events = events; }
    }

    // -----------------------------------------------------------------------
    // JSON helpers
    // -----------------------------------------------------------------------

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
