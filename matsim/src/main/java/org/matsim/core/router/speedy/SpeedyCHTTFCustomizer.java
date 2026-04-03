package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Link;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;

/**
 * Time-dependent customization of a {@link SpeedyCHGraph} using
 * Travel Time Functions (TTFs) – one {@code double[]} per edge per time bin.
 *
 * <p>After this customizer runs, {@link SpeedyCHGraph#ttf} and
 * {@link SpeedyCHGraph#minTTF} are populated and ready for use by
 * {@link SpeedyCHTimeDep}. {@link SpeedyCHGraph#edgeWeights} is also
 * overwritten with the per-edge minimum travel time (valid FIFO lower bound),
 * which the backward search uses as a conservative cost estimate.
 *
 * <h3>TTF assignment rules</h3>
 * <ul>
 *   <li><b>Real edges</b>: {@code ttf[e][k] = TravelTime.getLinkTravelTime(link, k * BIN_SIZE)}</li>
 *   <li><b>Shortcuts</b>: CATCHUp composition –
 *       {@code ttf[e][k] = ttf[lower1][k] + ttf[lower2][ bin(k*BIN_SIZE + ttf[lower1][k]) ]}</li>
 * </ul>
 *
 * <p>The composition is applied in edge-array order, which matches the
 * topological order guaranteed by {@link SpeedyCHBuilder} (real edges first,
 * then shortcuts ordered by their middle-node's contraction level).
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHTTFCustomizer {

    /** Number of time bins covering a full 24-hour day. */
    public static final int NUM_BINS = 96;

    /** Duration of each time bin in seconds (15 minutes). */
    public static final double BIN_SIZE = 900.0; // 15 min × 96 = 24 h

    /**
     * Populates {@link SpeedyCHGraph#ttf}, {@link SpeedyCHGraph#minTTF}, and
     * {@link SpeedyCHGraph#edgeWeights} in-place.
     *
     * @param chGraph the graph to customize
     * @param tt      provides time-dependent link travel times
     * @param td      provides {@code getLinkMinimumTravelDisutility} (used only as a
     *                cross-check; the main source of static lower bounds is the TTF minimum)
     */
    public void customize(SpeedyCHGraph chGraph, TravelTime tt, TravelDisutility td) {
        SpeedyGraph baseGraph = chGraph.getBaseGraph();
        int   edgeCount = chGraph.edgeCount;
        int[] edgeData  = chGraph.edgeData;

        // Reuse pre-allocated arrays from the CH graph (avoids per-iteration GC pressure).
        double[][] ttf    = chGraph.ttf;
        double[]   minTTF = chGraph.minTTF;

        for (int e = 0; e < edgeCount; e++) {
            int base     = e * SpeedyCHGraph.EDGE_SIZE;
            int origLink = edgeData[base + 4];
            double min   = Double.POSITIVE_INFINITY;

            if (origLink >= 0) {
                // ---- Real edge: sample TravelTime at each bin start ----
                Link link = baseGraph.getLink(origLink);
                for (int k = 0; k < NUM_BINS; k++) {
                    double t = tt.getLinkTravelTime(link, k * BIN_SIZE, null, null);
                    ttf[e][k] = t;
                    if (t < min) min = t;
                }
            } else {
                // ---- Shortcut: CATCHUp composition ----
                int lower1 = edgeData[base + 6];
                int lower2 = edgeData[base + 7];
                for (int k = 0; k < NUM_BINS; k++) {
                    double t1           = ttf[lower1][k];
                    double arrivalSecs  = k * BIN_SIZE + t1;
                    int    arrivalBin   = timeToBin(arrivalSecs);
                    double t2           = ttf[lower2][arrivalBin];
                    double composed     = t1 + t2;
                    ttf[e][k]  = composed;
                    if (composed < min) min = composed;
                }
            }

            minTTF[e] = min;
            // Write the per-edge minimum into edgeWeights for the backward lower-bound search.
            chGraph.edgeWeights[e] = min;
        }
    }

    /**
     * Maps an absolute time (seconds from midnight) to a TTF bin index,
     * wrapping around every 24 hours.
     */
    public static int timeToBin(double timeSecs) {
        int bin = (int) (timeSecs / BIN_SIZE) % NUM_BINS;
        if (bin < 0) bin += NUM_BINS;
        return bin;
    }
}
