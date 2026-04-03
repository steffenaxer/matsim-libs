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
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.scenario.ScenarioUtils;

/**
 * Basic sanity tests for {@link SpeedyCHCustomizer}.
 */
public class SpeedyCHCustomizerTest {

    /**
     * On a linear network A→B→C, after customization the weight of each real edge must
     * equal getLinkMinimumTravelDisutility for that link.
     */
    @Test
    void testRealEdgeWeights() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();

        Node nA = nf.createNode(Id.createNodeId("A"), new Coord(0, 0));
        Node nB = nf.createNode(Id.createNodeId("B"), new Coord(100, 0));
        Node nC = nf.createNode(Id.createNodeId("C"), new Coord(200, 0));
        network.addNode(nA); network.addNode(nB); network.addNode(nC);

        addLink(network, "AB", nA, nB, 100, 10);
        addLink(network, "BC", nB, nC, 100, 10);

        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        SpeedyCHGraph ch = new SpeedyCHBuilder(g, tc).build();
        new SpeedyCHCustomizer().customize(ch, tc);

        // Verify that all real edges have non-negative weights.
        for (int e = 0; e < ch.edgeCount; e++) {
            int origLink = ch.edgeData[e * SpeedyCHGraph.EDGE_SIZE + 4];
            if (origLink >= 0) {
                Link link = g.getLink(origLink);
                double expected = tc.getLinkMinimumTravelDisutility(link);
                Assertions.assertEquals(expected, ch.edgeWeights[e], 1e-9,
                        "Real edge weight mismatch for link " + link.getId());
            }
        }
    }

    /**
     * Shortcut weights must equal the sum of their sub-edge weights.
     */
    @Test
    void testShortcutWeights() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();

        // Create a diamond: A→B, A→C, B→D, C→D to encourage shortcut A→D
        Node nA = nf.createNode(Id.createNodeId("A"), new Coord(0, 0));
        Node nB = nf.createNode(Id.createNodeId("B"), new Coord(100, 100));
        Node nC = nf.createNode(Id.createNodeId("C"), new Coord(100, -100));
        Node nD = nf.createNode(Id.createNodeId("D"), new Coord(200, 0));
        network.addNode(nA); network.addNode(nB);
        network.addNode(nC); network.addNode(nD);

        addLink(network, "AB", nA, nB, 100, 10);
        addLink(network, "BD", nB, nD, 100, 10);
        addLink(network, "AC", nA, nC, 100, 10);
        addLink(network, "CD", nC, nD, 100, 10);

        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        SpeedyCHGraph ch = new SpeedyCHBuilder(g, tc).build();
        new SpeedyCHCustomizer().customize(ch, tc);

        // Verify shortcut weights.
        for (int e = 0; e < ch.edgeCount; e++) {
            int base     = e * SpeedyCHGraph.EDGE_SIZE;
            int origLink = ch.edgeData[base + 4];
            int lower1   = ch.edgeData[base + 6];
            int lower2   = ch.edgeData[base + 7];
            if (origLink < 0 && lower1 >= 0 && lower2 >= 0) {
                double expected = ch.edgeWeights[lower1] + ch.edgeWeights[lower2];
                Assertions.assertEquals(expected, ch.edgeWeights[e], 1e-9,
                        "Shortcut edge " + e + " weight mismatch");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
