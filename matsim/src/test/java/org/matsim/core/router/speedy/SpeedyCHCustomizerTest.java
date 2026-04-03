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
 * Basic sanity tests for {@link SpeedyCHTTFCustomizer}.
 */
public class SpeedyCHCustomizerTest {

    @Test
    void testRealEdgeTTFValues() {
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
        new SpeedyCHTTFCustomizer().customize(ch, tc, tc);

        Assertions.assertNotNull(ch.ttf,    "ttf array must be populated");
        Assertions.assertNotNull(ch.minTTF, "minTTF array must be populated");

        // For freespeed travel time the TTF is constant across all bins.
        int numBins = SpeedyCHTTFCustomizer.NUM_BINS;
        for (int e = 0; e < ch.totalEdgeCount; e++) {
            int origLink = ch.edgeOrigLink[e];
            if (origLink >= 0) {
                Link link = g.getLink(origLink);
                double expected = tc.getLinkTravelTime(link, 0.0, null, null);
                for (int k = 0; k < numBins; k++) {
                    Assertions.assertEquals(expected, ch.ttf[e * numBins + k], 1e-9,
                            "Real edge TTF bin " + k + " mismatch for link " + link.getId());
                }
                Assertions.assertEquals(expected, ch.minTTF[e], 1e-9,
                        "minTTF mismatch for link " + link.getId());
            }
        }
    }

    @Test
    void testShortcutTTFComposition() {
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Network network = scenario.getNetwork();
        NetworkFactory nf = network.getFactory();

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
        new SpeedyCHTTFCustomizer().customize(ch, tc, tc);

        int numBins = SpeedyCHTTFCustomizer.NUM_BINS;
        for (int e = 0; e < ch.totalEdgeCount; e++) {
            int orig   = ch.edgeOrigLink[e];
            int lower1 = ch.edgeLower1[e];
            int lower2 = ch.edgeLower2[e];
            if (orig < 0 && lower1 >= 0 && lower2 >= 0) {
                int eOff  = e * numBins;
                int l1Off = lower1 * numBins;
                int l2Off = lower2 * numBins;
                for (int k = 0; k < numBins; k++) {
                    double t1       = ch.ttf[l1Off + k];
                    int    arrBin   = SpeedyCHTTFCustomizer.timeToBin(k * SpeedyCHTTFCustomizer.BIN_SIZE + t1);
                    double t2       = ch.ttf[l2Off + arrBin];
                    double expected = t1 + t2;
                    Assertions.assertEquals(expected, ch.ttf[eOff + k], 1e-9,
                            "Shortcut " + e + " TTF bin " + k + " mismatch");
                }
            }
        }
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
