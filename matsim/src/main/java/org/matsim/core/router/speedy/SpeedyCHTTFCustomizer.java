package org.matsim.core.router.speedy;

import org.matsim.api.core.v01.network.Link;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;

/**
 * Time-dependent customization of a {@link SpeedyCHGraph} using
 * Travel Time Functions (TTFs).
 *
 * <p>After this customizer runs, {@link SpeedyCHGraph#ttf},
 * {@link SpeedyCHGraph#minTTF}, and {@link SpeedyCHGraph#edgeWeights}
 * are populated and ready for use by {@link SpeedyCHTimeDep}.
 *
 * <p>TTF storage is a flat contiguous {@code double[]} array:
 * {@code ttf[e * NUM_BINS + k]} for edge {@code e}, bin {@code k}.
 *
 * @author Implementation for CCH/CATCHUp router
 */
public class SpeedyCHTTFCustomizer {

    /** Number of time bins covering a full 24-hour day.
     *  Aligned with the MATSim default {@code travelTimeBinSize} of 900 s (15 min)
     *  in {@link org.matsim.core.config.groups.TravelTimeCalculatorConfigGroup}. */
    public static final int NUM_BINS = 96;

    /** Duration of each time bin in seconds (15 minutes).
     *  Matches the MATSim default {@code travelTimeBinSize} of 900 s. */
    public static final double BIN_SIZE = 900.0; // 15 min × 96 = 24 h

    /** Precomputed reciprocal for fast bin computation. */
    static final double INV_BIN_SIZE = 1.0 / BIN_SIZE;

    public void customize(SpeedyCHGraph chGraph, TravelTime tt, TravelDisutility td) {
        SpeedyGraph baseGraph = chGraph.getBaseGraph();
        int    edgeCount  = chGraph.totalEdgeCount;
        int[]  origLink   = chGraph.edgeOrigLink;
        int[]  lower1     = chGraph.edgeLower1;
        int[]  lower2     = chGraph.edgeLower2;
        double[] ttf      = chGraph.ttf;
        double[] minTTF   = chGraph.minTTF;
        double[] weights  = chGraph.edgeWeights;
        double[] ttfHash  = chGraph.ttfHash;
        boolean[] dirty   = new boolean[edgeCount];

        for (int e = 0; e < edgeCount; e++) {
            int eOff = e * NUM_BINS;
            double min = Double.POSITIVE_INFINITY;

            if (origLink[e] >= 0) {
                // Real edge: sample TravelTime at each bin start
                Link link = baseGraph.getLink(origLink[e]);
                double sum = 0;
                for (int k = 0; k < NUM_BINS; k++) {
                    double t = tt.getLinkTravelTime(link, k * BIN_SIZE, null, null);
                    ttf[eOff + k] = t;
                    sum += t;
                    if (t < min) min = t;
                }
                // Check if TTF changed since last customization
                if (sum != ttfHash[e]) {
                    dirty[e] = true;
                    ttfHash[e] = sum;
                }
            } else {
                // Shortcut: skip recomposition if both lower edges are unchanged
                int l1 = lower1[e];
                int l2 = lower2[e];
                if (!dirty[l1] && !dirty[l2]) {
                    // TTF unchanged — reuse cached values
                    continue;
                }
                dirty[e] = true;
                int l1Off = l1 * NUM_BINS;
                int l2Off = l2 * NUM_BINS;
                double sum = 0;
                for (int k = 0; k < NUM_BINS; k++) {
                    double t1         = ttf[l1Off + k];
                    double arrivalSec = k * BIN_SIZE + t1;
                    int    arrBin     = timeToBin(arrivalSec);
                    double t2         = ttf[l2Off + arrBin];
                    double composed   = t1 + t2;
                    ttf[eOff + k] = composed;
                    sum += composed;
                    if (composed < min) min = composed;
                }
                ttfHash[e] = sum;
            }

            minTTF[e]  = min;
            weights[e] = min;
        }

        // Propagate weights into colocated CSR weight arrays for cache-local access
        propagateWeightsToCSR(chGraph);
    }

    /**
     * Copies global edgeWeights into the colocated upWeights/dnWeights arrays
     * so that the query hot-path reads weight from the same cache region as
     * the target-node index.
     */
    static void propagateWeightsToCSR(SpeedyCHGraph chGraph) {
        int S = SpeedyCHGraph.E_STRIDE;
        // Upward edges
        int upTotal = chGraph.upEdgeCount;
        for (int slot = 0; slot < upTotal; slot++) {
            int gIdx = chGraph.upEdges[slot * S + SpeedyCHGraph.E_GIDX];
            chGraph.upWeights[slot] = chGraph.edgeWeights[gIdx];
        }
        // Downward edges
        int dnTotal = chGraph.dnEdgeCount;
        for (int slot = 0; slot < dnTotal; slot++) {
            int gIdx = chGraph.dnEdges[slot * S + SpeedyCHGraph.E_GIDX];
            chGraph.dnWeights[slot] = chGraph.edgeWeights[gIdx];
        }
    }

    /**
     * Maps an absolute time (seconds from midnight) to a TTF bin index,
     * wrapping around every 24 hours.
     */
    public static int timeToBin(double timeSecs) {
        int bin = (int) (timeSecs * INV_BIN_SIZE) % NUM_BINS;
        if (bin < 0) bin += NUM_BINS;
        return bin;
    }
}
