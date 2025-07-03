package benchmark;

import org.matsim.api.core.v01.network.*;
import org.matsim.api.core.v01.*;

public class GridNetworkGenerator {

    public static void generateNetwork(Scenario scenario) {
        // Create an empty MATSim network
        Network network = scenario.getNetwork();

        // Grid and link parameters
        double cellSize = 500.0; // meters
        int gridSize = 100; // 100 x 500m = 50 km
        double freespeed = 50.0 / 3.6; // 50 km/h in m/s
        double capacity = 1800.0; // vehicles per hour
        double lanes = 2.0;

        // Create nodes in a grid layout
        for (int i = 0; i <= gridSize; i++) {
            for (int j = 0; j <= gridSize; j++) {
                Id<Node> nodeId = Id.createNodeId("n_" + i + "_" + j);
                Node node = network.getFactory().createNode(nodeId, new Coord(i * cellSize, j * cellSize));
                network.addNode(node);
            }
        }

        // Create bidirectional links between neighboring nodes (right and down)
        int linkIdCounter = 0;
        for (int i = 0; i < gridSize; i++) {
            for (int j = 0; j < gridSize; j++) {
                Node from = network.getNodes().get(Id.createNodeId("n_" + i + "_" + j));
                Node right = network.getNodes().get(Id.createNodeId("n_" + (i + 1) + "_" + j));
                Node down = network.getNodes().get(Id.createNodeId("n_" + i + "_" + (j + 1)));

                // Link to the right neighbor
                createLink(network, from, right, linkIdCounter++, cellSize, freespeed, capacity, lanes);
                createLink(network, right, from, linkIdCounter++, cellSize, freespeed, capacity, lanes);

                // Link to the bottom neighbor
                createLink(network, from, down, linkIdCounter++, cellSize, freespeed, capacity, lanes);
                createLink(network, down, from, linkIdCounter++, cellSize, freespeed, capacity, lanes);
            }
        }
    }

    // Helper method to create and add a link to the network
    private static void createLink(Network network, Node from, Node to, int id,
                                   double length, double freespeed, double capacity, double lanes) {
        Id<Link> linkId = Id.createLinkId("l_" + id);
        Link link = network.getFactory().createLink(linkId, from, to);
        link.setLength(length);
        link.setFreespeed(freespeed);
        link.setCapacity(capacity);
        link.setNumberOfLanes(lanes);
        network.addLink(link);
    }
}
