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
 * Basic sanity tests for {@link SpeedyCHBuilder}.
 */
public class SpeedyCHBuilderTest {

    @Test
    void testBuildLinearNetwork() {
        Network network = buildLinearNetwork(4);
        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        SpeedyCHGraph ch = new SpeedyCHBuilder(g, tc).build();

        Assertions.assertEquals(g.nodeCount, ch.nodeCount);
        Assertions.assertTrue(ch.totalEdgeCount >= g.linkCount,
                "CH should have at least as many edges as the base graph");
    }

    @Test
    void testBuildTriangleNetwork() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();

        Node nA = nf.createNode(Id.createNodeId("A"), new Coord(0, 0));
        Node nB = nf.createNode(Id.createNodeId("B"), new Coord(100, 0));
        Node nC = nf.createNode(Id.createNodeId("C"), new Coord(200, 0));
        network.addNode(nA); network.addNode(nB); network.addNode(nC);

        addLink(network, "AB", nA, nB, 100, 10);
        addLink(network, "BC", nB, nC, 100, 10);
        addLink(network, "AC", nA, nC, 300, 10);

        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        SpeedyCHGraph ch = new SpeedyCHBuilder(g, tc).build();
        new SpeedyCHTTFCustomizer().customize(ch, tc, tc);

        SpeedyCHTimeDep router = new SpeedyCHTimeDep(ch, tc, tc);
        SpeedyCHTimeDep.Path path = router.calcLeastCostPath(nA, nC, 0, null, null);

        Assertions.assertNotNull(path, "Path should not be null");
        Assertions.assertEquals(2, path.links.size(), "Expected path A→B→C (2 links)");
    }

    @Test
    void testSingleNodeNetwork() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();
        Node n = nf.createNode(Id.createNodeId("N"), new Coord(0, 0));
        network.addNode(n);

        FreespeedTravelTimeAndDisutility tc = new FreespeedTravelTimeAndDisutility(new ScoringConfigGroup());
        SpeedyGraph g = SpeedyGraphBuilder.build(network);
        SpeedyCHGraph ch = new SpeedyCHBuilder(g, tc).build();
        Assertions.assertNotNull(ch);
    }

    private static Network buildLinearNetwork(int nodeCount) {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();

        Node[] nodes = new Node[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            nodes[i] = nf.createNode(Id.createNodeId(String.valueOf(i)), new Coord(i * 100, 0));
            network.addNode(nodes[i]);
        }
        for (int i = 0; i < nodeCount - 1; i++) {
            addLink(network, i + "->" + (i + 1), nodes[i], nodes[i + 1], 100, 10);
        }
        return network;
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
}
