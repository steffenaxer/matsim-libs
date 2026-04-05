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
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.misc.OptionalTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Tests for {@link SpeedyCHLeastCostPathTree}: verifies that the CH-based
 * one-to-all search produces the same costs as the Dijkstra-based
 * {@link LeastCostPathTree}.
 */
public class SpeedyCHLeastCostPathTreeTest {

    private static final int    NUM_SOURCE_NODES = 20;
    private static final int    NUM_TARGET_NODES = 50;
    private static final double COST_TOLERANCE   = 1e-3;

    @Test
    void testForwardSearchSmallGrid() {
        Network network = buildGridNetwork(5);
        runForwardCorrectnessTest(network);
    }

    @Test
    void testForwardSearchLargerGrid() {
        Network network = buildGridNetwork(15);
        runForwardCorrectnessTest(network);
    }

    @Test
    void testForwardSearchEquilNetwork() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(scenario.getNetwork()).readFile("test/scenarios/equil/network.xml");
        runForwardCorrectnessTest(scenario.getNetwork());
    }

    @Test
    void testForwardSearchBerlinNetwork() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(scenario.getNetwork()).readFile("test/scenarios/berlin/network.xml.gz");
        runForwardCorrectnessTest(scenario.getNetwork());
    }

    @Test
    void testBackwardSearchSmallGrid() {
        Network network = buildGridNetwork(5);
        runBackwardCorrectnessTest(network);
    }

    @Test
    void testBackwardSearchLargerGrid() {
        Network network = buildGridNetwork(15);
        runBackwardCorrectnessTest(network);
    }

    @Test
    void testBackwardSearchBerlinNetwork() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(scenario.getNetwork()).readFile("test/scenarios/berlin/network.xml.gz");
        runBackwardCorrectnessTest(scenario.getNetwork());
    }

    // ---- forward search correctness ----

    private void runForwardCorrectnessTest(Network network) {
        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(
                new ScoringConfigGroup());

        SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);

        // Build CH
        InertialFlowCutter.NDOrderResult orderResult =
                new InertialFlowCutter(baseGraph).computeOrderWithBatches();
        SpeedyCHGraph chGraph = new SpeedyCHBuilder(baseGraph, tc).buildWithOrderParallel(orderResult);
        new SpeedyCHTTFCustomizer().customize(chGraph, tc, tc);

        // CH tree
        SpeedyCHLeastCostPathTree chTree = new SpeedyCHLeastCostPathTree(chGraph, tc, tc);

        // Reference Dijkstra tree
        LeastCostPathTree dijkstraTree = new LeastCostPathTree(baseGraph, tc, tc);

        List<Link> linkList = new ArrayList<>(network.getLinks().values());
        int n = linkList.size();
        if (n < 2) return;

        Random rng = new Random(42);
        int mismatches = 0;
        int comparisons = 0;

        for (int s = 0; s < NUM_SOURCE_NODES; s++) {
            Link srcLink = linkList.get(rng.nextInt(n));
            double startTime = 8.0 * 3600;

            // Run both trees from the same source
            chTree.calculate(srcLink, startTime, null, null);
            dijkstraTree.calculate(srcLink, startTime, null, null);

            // Compare travel times at random target nodes
            // (CH tree uses TTF-based travel time as cost, matching SpeedyCHTimeDep behavior)
            for (int t = 0; t < NUM_TARGET_NODES; t++) {
                Link tgtLink = linkList.get(rng.nextInt(n));
                int tgtNodeIdx = tgtLink.getFromNode().getId().index();

                OptionalTime chTimeOpt = chTree.getTime(tgtNodeIdx);
                OptionalTime dijTimeOpt = dijkstraTree.getTime(tgtNodeIdx);

                // Both should be defined or both undefined
                if (chTimeOpt.isUndefined() && dijTimeOpt.isUndefined()) {
                    comparisons++;
                    continue;
                }

                if (chTimeOpt.isUndefined() || dijTimeOpt.isUndefined()) {
                    if (dijTimeOpt.isDefined() && chTimeOpt.isUndefined()) {
                        mismatches++;
                        System.err.printf("FORWARD MISMATCH src=%s tgt=%s: CH=unreachable  Dijkstra=%.6f%n",
                                srcLink.getId(), tgtLink.getId(), dijTimeOpt.seconds());
                    }
                    comparisons++;
                    continue;
                }

                comparisons++;
                double chTime = chTimeOpt.seconds() - startTime;
                double dijTime = dijTimeOpt.seconds() - startTime;

                if (Math.abs(chTime - dijTime) > COST_TOLERANCE) {
                    mismatches++;
                    System.err.printf("FORWARD MISMATCH src=%s tgt=%s: CH_time=%.6f  Dijkstra_time=%.6f%n",
                            srcLink.getId(), tgtLink.getId(), chTime, dijTime);
                }
            }
        }

        Assertions.assertEquals(0, mismatches,
                mismatches + " forward cost mismatches out of " + comparisons
                        + " comparisons with CH LeastCostPathTree.");
    }

    // ---- backward search correctness ----

    private void runBackwardCorrectnessTest(Network network) {
        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(
                new ScoringConfigGroup());

        SpeedyGraph baseGraph = SpeedyGraphBuilder.build(network);

        InertialFlowCutter.NDOrderResult orderResult =
                new InertialFlowCutter(baseGraph).computeOrderWithBatches();
        SpeedyCHGraph chGraph = new SpeedyCHBuilder(baseGraph, tc).buildWithOrderParallel(orderResult);
        new SpeedyCHTTFCustomizer().customize(chGraph, tc, tc);

        SpeedyCHLeastCostPathTree chTree = new SpeedyCHLeastCostPathTree(chGraph, tc, tc);
        LeastCostPathTree dijkstraTree = new LeastCostPathTree(baseGraph, tc, tc);

        List<Link> linkList = new ArrayList<>(network.getLinks().values());
        int n = linkList.size();
        if (n < 2) return;

        Random rng = new Random(42);
        int mismatches = 0;
        int comparisons = 0;

        for (int s = 0; s < NUM_SOURCE_NODES; s++) {
            Link arrivalLink = linkList.get(rng.nextInt(n));
            double arrivalTime = 8.0 * 3600;

            chTree.calculateBackwards(arrivalLink, arrivalTime, null, null);
            dijkstraTree.calculateBackwards(arrivalLink, arrivalTime, null, null);

            for (int t = 0; t < NUM_TARGET_NODES; t++) {
                Link srcLink = linkList.get(rng.nextInt(n));
                int srcNodeIdx = srcLink.getToNode().getId().index();

                OptionalTime chTimeOpt = chTree.getTime(srcNodeIdx);
                OptionalTime dijTimeOpt = dijkstraTree.getTime(srcNodeIdx);

                if (chTimeOpt.isUndefined() && dijTimeOpt.isUndefined()) {
                    comparisons++;
                    continue;
                }

                if (chTimeOpt.isUndefined() || dijTimeOpt.isUndefined()) {
                    if (dijTimeOpt.isDefined() && chTimeOpt.isUndefined()) {
                        mismatches++;
                        System.err.printf("BACKWARD MISMATCH arrival=%s src=%s: CH=unreachable  Dijkstra=%.6f%n",
                                arrivalLink.getId(), srcLink.getId(), dijTimeOpt.seconds());
                    }
                    comparisons++;
                    continue;
                }

                comparisons++;
                double chTime = arrivalTime - chTimeOpt.seconds();
                double dijTime = arrivalTime - dijTimeOpt.seconds();

                if (Math.abs(chTime - dijTime) > COST_TOLERANCE) {
                    mismatches++;
                    System.err.printf("BACKWARD MISMATCH arrival=%s src=%s: CH_time=%.6f  Dijkstra_time=%.6f%n",
                            arrivalLink.getId(), srcLink.getId(), chTime, dijTime);
                }
            }
        }

        Assertions.assertEquals(0, mismatches,
                mismatches + " backward cost mismatches out of " + comparisons
                        + " comparisons with CH LeastCostPathTree.");
    }

    // ---- network builders ----

    private static Network buildGridNetwork(int size) {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();

        Node[][] nodes = new Node[size][size];
        for (int x = 0; x < size; x++) {
            for (int y = 0; y < size; y++) {
                String id = x + "_" + y;
                nodes[x][y] = nf.createNode(Id.createNodeId(id), new Coord(x * 1000, y * 1000));
                network.addNode(nodes[x][y]);
            }
        }

        int linkId = 0;
        for (int x = 0; x < size; x++) {
            for (int y = 0; y < size; y++) {
                if (x + 1 < size) {
                    addBidirectionalLink(network, nf, linkId++, nodes[x][y], nodes[x + 1][y]);
                    linkId++;
                }
                if (y + 1 < size) {
                    addBidirectionalLink(network, nf, linkId++, nodes[x][y], nodes[x][y + 1]);
                    linkId++;
                }
            }
        }

        return network;
    }

    private static void addBidirectionalLink(Network network, NetworkFactory nf,
                                              int baseLinkId, Node from, Node to) {
        double length = Math.sqrt(
                Math.pow(from.getCoord().getX() - to.getCoord().getX(), 2) +
                Math.pow(from.getCoord().getY() - to.getCoord().getY(), 2));
        Link fwd = nf.createLink(Id.createLinkId(baseLinkId), from, to);
        fwd.setLength(length);
        fwd.setFreespeed(13.89);
        fwd.setCapacity(1000);
        fwd.setNumberOfLanes(1);
        network.addLink(fwd);

        Link bwd = nf.createLink(Id.createLinkId(baseLinkId + 1), to, from);
        bwd.setLength(length);
        bwd.setFreespeed(13.89);
        bwd.setCapacity(1000);
        bwd.setNumberOfLanes(1);
        network.addLink(bwd);
    }
}
