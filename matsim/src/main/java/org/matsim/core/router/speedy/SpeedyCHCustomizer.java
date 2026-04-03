package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Link;
import org.matsim.core.router.util.TravelDisutility;

/**
 * Assigns edge weights to a {@link SpeedyCHGraph} (the "customization" step of CCH/CH).
 *
 * <ul>
 *   <li><b>Real edges</b>: weight = {@link TravelDisutility#getLinkMinimumTravelDisutility(Link)}</li>
 *   <li><b>Shortcuts</b>: weight = weight[lowerEdge1] + weight[lowerEdge2]</li>
 * </ul>
 *
 * <p>Edges are processed in the order they were added during contraction.
 * Original edges come first; shortcuts are added in order of their middle-node's
 * contraction level, which guarantees that a shortcut's sub-edges are always
 * customized before the shortcut itself.
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHCustomizer {

    /**
     * Fills {@link SpeedyCHGraph#edgeWeights} in-place using {@code td}.
     *
     * @param chGraph the graph whose weights will be set
     * @param td      provides {@link TravelDisutility#getLinkMinimumTravelDisutility}
     */
    public void customize(SpeedyCHGraph chGraph, TravelDisutility td) {
        SpeedyGraph baseGraph = chGraph.getBaseGraph();
        int edgeCount = chGraph.edgeCount;
        double[] weights = chGraph.edgeWeights;
        int[] edgeData = chGraph.edgeData;

        for (int e = 0; e < edgeCount; e++) {
            int base     = e * SpeedyCHGraph.EDGE_SIZE;
            int origLink = edgeData[base + 4];

            if (origLink >= 0) {
                // Real edge: use static minimum travel disutility.
                Link link = baseGraph.getLink(origLink);
                weights[e] = td.getLinkMinimumTravelDisutility(link);
            } else {
                // Shortcut: propagate sub-edge weights (both sub-edges already customized).
                int lower1 = edgeData[base + 6];
                int lower2 = edgeData[base + 7];
                weights[e] = weights[lower1] + weights[lower2];
            }
        }
    }
}
