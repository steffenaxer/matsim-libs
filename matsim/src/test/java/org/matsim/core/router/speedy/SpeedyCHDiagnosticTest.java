package org.matsim.core.router.speedy;

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

/**
 * Diagnostic test: compare sequential vs parallel ND contraction edge counts.
 */
public class SpeedyCHDiagnosticTest {

    @Test
    void compareSeqVsParallelBerlin() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(scenario.getNetwork())
                .readFile("test/scenarios/berlin/network.xml.gz");
        compare(scenario.getNetwork(), "Berlin-11k");
    }

    @Test
    void compareSeqVsParallelGrid15() {
        compare(buildGridNetwork(15), "Grid-15x15");
    }

    @Test
    void compareSeqVsParallelGrid30() {
        compare(buildGridNetwork(30), "Grid-30x30");
    }

    private void compare(Network network, String label) {
        var tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        System.out.printf("%n=== %s: %d nodes, %d links ===%n", label, g.nodeCount, g.linkCount);

        // Compute ND order (same for both)
        InertialFlowCutter.NDOrderResult ndOrder = new InertialFlowCutter(g).computeOrderWithBatches();

        // Sequential ND
        SpeedyCHGraph chSeq = new SpeedyCHBuilder(g, tc).buildWithOrder(ndOrder.order);
        System.out.printf("  Sequential ND: %,d total edges (%.1f%% overhead vs base)%n",
                chSeq.totalEdgeCount, ((double) chSeq.totalEdgeCount / g.linkCount - 1) * 100);

        // Parallel ND (same order)
        SpeedyCHGraph chPar = new SpeedyCHBuilder(g, tc).buildWithOrderParallel(ndOrder);
        System.out.printf("  Parallel ND:   %,d total edges (%.1f%% overhead vs base)%n",
                chPar.totalEdgeCount, ((double) chPar.totalEdgeCount / g.linkCount - 1) * 100);

        int diff = chPar.totalEdgeCount - chSeq.totalEdgeCount;
        System.out.printf("  Difference:    %+,d edges (%+.2f%%)%n", diff,
                ((double) chPar.totalEdgeCount / chSeq.totalEdgeCount - 1) * 100);
    }

    private static Network buildGridNetwork(int n) {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();
        Node[][] nodes = new Node[n][n];
        for (int r = 0; r < n; r++)
            for (int c = 0; c < n; c++) {
                nodes[r][c] = nf.createNode(Id.createNodeId(r + "_" + c), new Coord(c * 100, r * 100));
                network.addNode(nodes[r][c]);
            }
        for (int r = 0; r < n; r++)
            for (int c = 0; c < n; c++) {
                if (c + 1 < n) {
                    addLink(network, r + "_" + c + "R", nodes[r][c], nodes[r][c + 1], 100, 10);
                    addLink(network, r + "_" + c + "L", nodes[r][c + 1], nodes[r][c], 100, 10);
                }
                if (r + 1 < n) {
                    addLink(network, r + "_" + c + "D", nodes[r][c], nodes[r + 1][c], 100, 10);
                    addLink(network, r + "_" + c + "U", nodes[r + 1][c], nodes[r][c], 100, 10);
                }
            }
        return network;
    }

    private static void addLink(Network network, String id, Node from, Node to,
                                double length, double freespeed) {
        NetworkFactory nf = network.getFactory();
        Link link = nf.createLink(Id.createLinkId(id), from, to);
        link.setLength(length); link.setFreespeed(freespeed);
        link.setCapacity(1800); link.setNumberOfLanes(1);
        network.addLink(link);
    }
}
