/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** */

package org.matsim.contrib.drt.extension.benchmark.traveltime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.router.util.TravelTime;
import org.matsim.vehicles.Vehicle;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adaptive TravelTime wrapper that clusters link travel time patterns for better cache efficiency.
 * <p>
 * This class wraps an existing {@link TravelTime} implementation and periodically re-clusters
 * the travel time patterns to adapt to changing traffic conditions.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Wraps any existing TravelTime implementation</li>
 *   <li>Periodically rebuilds clusters based on current data</li>
 *   <li>~100x less memory than storing full time series per link</li>
 *   <li>Better cache locality through pattern sharing</li>
 * </ul>
 * <p>
 * <b>Usage:</b>
 * <pre>
 * // Wrap existing TravelTime
 * TravelTime baseTT = travelTimeCalculator.getLinkTravelTimes();
 * AdaptiveClusteredTravelTime clustered = new AdaptiveClusteredTravelTime(network, baseTT, config);
 *
 * // Use in routing
 * router.setTravelTime(clustered);
 *
 * // Periodically update clusters (e.g., every iteration or every N seconds)
 * clustered.rebuildClusters();
 * </pre>
 *
 * @author Steffen Axer
 */
public class AdaptiveClusteredTravelTime implements TravelTime {
	private static final Logger LOG = LogManager.getLogger(AdaptiveClusteredTravelTime.class);

	/**
	 * Configuration for the adaptive clustering.
	 */
	public static class Config {
		private int numPatterns = 256;
		private int numBins = 96;           // 24h with 15min bins
		private double binSizeSeconds = 900; // 15 minutes
		private int maxClusteringIterations = 50;
		private double convergenceThreshold = 0.001;

		public Config() {}

		public Config setNumPatterns(int numPatterns) {
			this.numPatterns = numPatterns;
			return this;
		}

		public Config setNumBins(int numBins) {
			this.numBins = numBins;
			return this;
		}

		public Config setBinSizeSeconds(double binSizeSeconds) {
			this.binSizeSeconds = binSizeSeconds;
			return this;
		}

		public Config setMaxClusteringIterations(int maxIterations) {
			this.maxClusteringIterations = maxIterations;
			return this;
		}

		public int getNumPatterns() { return numPatterns; }
		public int getNumBins() { return numBins; }
		public double getBinSizeSeconds() { return binSizeSeconds; }
		public int getMaxClusteringIterations() { return maxClusteringIterations; }
	}

	private final Network network;
	private final TravelTime delegate;
	private final Config config;
	private final int linkCount;

	// Clustered data - volatile for thread-safe reads during rebuild
	private volatile ClusterData clusterData;

	// Flag to prevent concurrent rebuilds
	private final AtomicBoolean rebuildInProgress = new AtomicBoolean(false);

	/**
	 * Creates a new AdaptiveClusteredTravelTime with default configuration.
	 */
	public AdaptiveClusteredTravelTime(Network network, TravelTime delegate) {
		this(network, delegate, new Config());
	}

	/**
	 * Creates a new AdaptiveClusteredTravelTime with custom configuration.
	 */
	public AdaptiveClusteredTravelTime(Network network, TravelTime delegate, Config config) {
		this.network = network;
		this.delegate = delegate;
		this.config = config;
		this.linkCount = Id.getNumberOfIds(Link.class);

		// Initial cluster build
		rebuildClusters();
	}

	@Override
	public double getLinkTravelTime(Link link, double time, Person person, Vehicle vehicle) {
		ClusterData data = this.clusterData;

		// Fallback to delegate if clusters not yet built
		if (data == null) {
			return delegate.getLinkTravelTime(link, time, person, vehicle);
		}

		int linkIdx = link.getId().index();

		// Bounds check
		if (linkIdx < 0 || linkIdx >= data.linkPatternIds.length) {
			return delegate.getLinkTravelTime(link, time, person, vehicle);
		}

		// Fast path: clustered lookup
		int patternId = data.linkPatternIds[linkIdx] & 0xFFFF;
		float baseTT = data.linkBaseTravelTimes[linkIdx];

		int bin = Math.min(Math.max(0, (int) (time / config.binSizeSeconds)), config.numBins - 1);
		float factor = data.patterns[patternId][bin];

		double tt = baseTT * factor;

		// Apply vehicle speed limit if applicable
		if (vehicle != null && vehicle.getType() != null) {
			double vehicleTT = link.getLength() / vehicle.getType().getMaximumVelocity();
			tt = Math.max(tt, vehicleTT);
		}

		return tt;
	}

	/**
	 * Rebuilds the clusters based on current travel time data from the delegate.
	 * <p>
	 * This method should be called periodically to adapt to changing traffic patterns.
	 * It is thread-safe and will skip if a rebuild is already in progress.
	 */
	public void rebuildClusters() {
		if (!rebuildInProgress.compareAndSet(false, true)) {
			LOG.debug("Cluster rebuild already in progress, skipping");
			return;
		}

		try {
			LOG.info("Rebuilding travel time clusters for {} links with {} patterns...",
				linkCount, config.numPatterns);
			long startTime = System.currentTimeMillis();

			ClusterData newData = buildClusters();
			this.clusterData = newData;

			long elapsed = System.currentTimeMillis() - startTime;
			LOG.info("Cluster rebuild completed in {} ms. Memory: {}", elapsed, getMemoryComparisonString());
		} finally {
			rebuildInProgress.set(false);
		}
	}

	private ClusterData buildClusters() {
		int numBins = config.numBins;
		int numPatterns = Math.min(config.numPatterns, linkCount);

		// 1. Extract normalized profiles from delegate
		float[][] normalizedProfiles = new float[linkCount][numBins];
		float[] baseTravelTimes = new float[linkCount];
		boolean[] validLinks = new boolean[linkCount];

		for (Link link : network.getLinks().values()) {
			int linkIdx = link.getId().index();
			if (linkIdx < 0 || linkIdx >= linkCount) continue;

			double freeSpeedTT = link.getLength() / link.getFreespeed();
			if (freeSpeedTT <= 0 || Double.isNaN(freeSpeedTT) || Double.isInfinite(freeSpeedTT)) {
				freeSpeedTT = 1.0; // Fallback
			}
			baseTravelTimes[linkIdx] = (float) freeSpeedTT;
			validLinks[linkIdx] = true;

			for (int bin = 0; bin < numBins; bin++) {
				double time = bin * config.binSizeSeconds + config.binSizeSeconds / 2;
				double tt = delegate.getLinkTravelTime(link, time, null, null);

				// Factor relative to FreeSpeed
				float factor = (float) (tt / freeSpeedTT);
				if (Float.isNaN(factor) || Float.isInfinite(factor) || factor < 0) {
					factor = 1.0f;
				}
				normalizedProfiles[linkIdx][bin] = factor;
			}
		}

		// 2. K-Means clustering
		ClusteringResult result = kMeansClustering(normalizedProfiles, validLinks, numPatterns, numBins);

		return new ClusterData(result.centroids, result.assignments, baseTravelTimes);
	}

	private ClusteringResult kMeansClustering(float[][] profiles, boolean[] validLinks,
			int k, int dimensions) {
		Random random = new Random(42);

		// Count valid links
		int validCount = 0;
		for (boolean valid : validLinks) {
			if (valid) validCount++;
		}

		if (validCount == 0) {
			// Return trivial clustering
			float[][] centroids = new float[1][dimensions];
			Arrays.fill(centroids[0], 1.0f);
			short[] assignments = new short[profiles.length];
			return new ClusteringResult(centroids, assignments);
		}

		k = Math.min(k, validCount);

		// Initialize centroids with K-Means++
		float[][] centroids = initializeCentroids(profiles, validLinks, k, dimensions, random);
		short[] assignments = new short[profiles.length];
		Arrays.fill(assignments, (short) -1);

		// Iterative refinement
		for (int iter = 0; iter < config.maxClusteringIterations; iter++) {
			// Assignment step
			boolean changed = false;
			for (int i = 0; i < profiles.length; i++) {
				if (!validLinks[i]) continue;

				short nearest = findNearestCentroid(profiles[i], centroids);
				if (nearest != assignments[i]) {
					assignments[i] = nearest;
					changed = true;
				}
			}

			if (!changed) {
				LOG.debug("K-Means converged after {} iterations", iter);
				break;
			}

			// Update step
			updateCentroids(profiles, validLinks, assignments, centroids, dimensions);
		}

		return new ClusteringResult(centroids, assignments);
	}

	private float[][] initializeCentroids(float[][] profiles, boolean[] validLinks,
			int k, int dimensions, Random random) {
		float[][] centroids = new float[k][dimensions];
		boolean[] chosenIndices = new boolean[profiles.length];

		// First centroid: random valid link
		int first = -1;
		for (int attempts = 0; attempts < 1000 && first < 0; attempts++) {
			int candidate = random.nextInt(profiles.length);
			if (validLinks[candidate]) {
				first = candidate;
			}
		}
		if (first < 0) {
			// Fallback: find first valid
			for (int i = 0; i < validLinks.length; i++) {
				if (validLinks[i]) { first = i; break; }
			}
		}

		System.arraycopy(profiles[first], 0, centroids[0], 0, dimensions);
		chosenIndices[first] = true;

		// K-Means++ for remaining centroids
		float[] distances = new float[profiles.length];
		for (int c = 1; c < k; c++) {
			float totalDist = 0;
			for (int i = 0; i < profiles.length; i++) {
				if (!validLinks[i] || chosenIndices[i]) {
					distances[i] = 0;
					continue;
				}
				float minDist = Float.MAX_VALUE;
				for (int j = 0; j < c; j++) {
					float dist = squaredDistance(profiles[i], centroids[j]);
					minDist = Math.min(minDist, dist);
				}
				distances[i] = minDist;
				totalDist += minDist;
			}

			if (totalDist <= 0) break;

			float threshold = random.nextFloat() * totalDist;
			float cumulative = 0;
			for (int i = 0; i < profiles.length; i++) {
				if (!validLinks[i] || chosenIndices[i]) continue;
				cumulative += distances[i];
				if (cumulative >= threshold) {
					System.arraycopy(profiles[i], 0, centroids[c], 0, dimensions);
					chosenIndices[i] = true;
					break;
				}
			}
		}

		return centroids;
	}

	private short findNearestCentroid(float[] profile, float[][] centroids) {
		short nearest = 0;
		float minDist = Float.MAX_VALUE;
		for (int c = 0; c < centroids.length; c++) {
			float dist = squaredDistance(profile, centroids[c]);
			if (dist < minDist) {
				minDist = dist;
				nearest = (short) c;
			}
		}
		return nearest;
	}

	private void updateCentroids(float[][] profiles, boolean[] validLinks,
			short[] assignments, float[][] centroids, int dimensions) {
		int k = centroids.length;
		int[] counts = new int[k];
		float[][] sums = new float[k][dimensions];

		for (int i = 0; i < profiles.length; i++) {
			if (!validLinks[i] || assignments[i] < 0) continue;
			int cluster = assignments[i];
			counts[cluster]++;
			for (int d = 0; d < dimensions; d++) {
				sums[cluster][d] += profiles[i][d];
			}
		}

		for (int c = 0; c < k; c++) {
			if (counts[c] > 0) {
				for (int d = 0; d < dimensions; d++) {
					centroids[c][d] = sums[c][d] / counts[c];
				}
			} else {
				// Keep existing centroid or set to neutral
				Arrays.fill(centroids[c], 1.0f);
			}
		}
	}

	private static float squaredDistance(float[] a, float[] b) {
		float sum = 0;
		for (int i = 0; i < a.length; i++) {
			float diff = a[i] - b[i];
			sum += diff * diff;
		}
		return sum;
	}

	// ========== Statistics ==========

	/**
	 * Returns memory usage in bytes.
	 */
	public long getMemoryUsageBytes() {
		ClusterData data = this.clusterData;
		if (data == null) return 0;

		long patternMem = (long) data.patterns.length * data.patterns[0].length * 4;
		long linkPatternMem = (long) data.linkPatternIds.length * 2;
		long linkBaseMem = (long) data.linkBaseTravelTimes.length * 4;
		return patternMem + linkPatternMem + linkBaseMem;
	}

	/**
	 * Returns a comparison string showing memory savings.
	 */
	public String getMemoryComparisonString() {
		long clusteredMem = getMemoryUsageBytes();
		long arrayMem = (long) linkCount * config.numBins * 8;
		double ratio = clusteredMem > 0 ? (double) arrayMem / clusteredMem : 0;
		return String.format("Clustered: %.2f MB, Full: %.2f MB, Ratio: %.1fx smaller",
			clusteredMem / (1024.0 * 1024.0),
			arrayMem / (1024.0 * 1024.0),
			ratio);
	}

	/**
	 * Returns the wrapped delegate TravelTime.
	 */
	public TravelTime getDelegate() {
		return delegate;
	}

	/**
	 * Returns the current number of patterns.
	 */
	public int getNumPatterns() {
		ClusterData data = this.clusterData;
		return data != null ? data.patterns.length : 0;
	}

	// ========== Internal Data Structures ==========

	private static class ClusterData {
		final float[][] patterns;
		final short[] linkPatternIds;
		final float[] linkBaseTravelTimes;

		ClusterData(float[][] patterns, short[] linkPatternIds, float[] linkBaseTravelTimes) {
			this.patterns = patterns;
			this.linkPatternIds = linkPatternIds;
			this.linkBaseTravelTimes = linkBaseTravelTimes;
		}
	}

	private record ClusteringResult(float[][] centroids, short[] assignments) {}
}

