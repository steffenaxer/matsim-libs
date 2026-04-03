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
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHCustomizer {

    public void customize(SpeedyCHGraph chGraph, TravelDisutility td) {
        SpeedyGraph baseGraph = chGraph.getBaseGraph();
        int      edgeCount = chGraph.totalEdgeCount;
        double[] weights   = chGraph.edgeWeights;
        int[]    origLink  = chGraph.edgeOrigLink;
        int[]    lower1    = chGraph.edgeLower1;
        int[]    lower2    = chGraph.edgeLower2;

        for (int e = 0; e < edgeCount; e++) {
            if (origLink[e] >= 0) {
                Link link = baseGraph.getLink(origLink[e]);
                weights[e] = td.getLinkMinimumTravelDisutility(link);
            } else {
                weights[e] = weights[lower1[e]] + weights[lower2[e]];
            }
        }
    }
}
