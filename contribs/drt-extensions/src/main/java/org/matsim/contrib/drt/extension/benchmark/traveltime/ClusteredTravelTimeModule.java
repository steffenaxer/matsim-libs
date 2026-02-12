/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** */

package org.matsim.contrib.drt.extension.benchmark.traveltime;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.Config;
import org.matsim.core.controler.AbstractModule;
import org.matsim.core.mobsim.framework.events.MobsimBeforeSimStepEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimBeforeSimStepListener;
import org.matsim.core.router.util.TravelTime;
import org.matsim.core.trafficmonitoring.TravelTimeCalculator;

import java.util.Collection;
import java.util.Set;

/**
 * Module that installs {@link AdaptiveClusteredTravelTime} as the TravelTime for all network modes.
 * <p>
 * The clustered TravelTime wraps the standard TravelTimeCalculator and provides
 * ~100x less memory usage with 2-4x faster lookups through pattern clustering.
 * <p>
 * Usage:
 * <pre>
 * controler.addOverridingModule(new ClusteredTravelTimeModule(256));
 * </pre>
 *
 * @author Steffen Axer
 */
public class ClusteredTravelTimeModule extends AbstractModule {
	private static final Logger LOG = LogManager.getLogger(ClusteredTravelTimeModule.class);

	private final int numPatterns;
	private final int rebuildIntervalSeconds;

	/**
	 * Creates module with default settings (256 patterns, no rebuild during sim).
	 */
	public ClusteredTravelTimeModule() {
		this(256, 0);
	}

	/**
	 * Creates module with custom number of patterns.
	 *
	 * @param numPatterns Number of pattern clusters (more = more accuracy)
	 */
	public ClusteredTravelTimeModule(int numPatterns) {
		this(numPatterns, 0);
	}

	/**
	 * Creates module with custom settings.
	 *
	 * @param numPatterns            Number of pattern clusters
	 * @param rebuildIntervalSeconds Interval for rebuilding clusters (0 = never rebuild during sim)
	 */
	public ClusteredTravelTimeModule(int numPatterns, int rebuildIntervalSeconds) {
		this.numPatterns = numPatterns;
		this.rebuildIntervalSeconds = rebuildIntervalSeconds;
	}

	@Override
	public void install() {
		LOG.info("Installing ClusteredTravelTimeModule with {} patterns", numPatterns);

		Config config = getConfig();
		Collection<String> networkModes = config.routing().getNetworkModes();
		Set<String> analyzedModes = config.travelTimeCalculator().getAnalyzedModes();

		// Bind the AdaptiveClusteredTravelTime as singleton
		bind(AdaptiveClusteredTravelTime.class)
			.toProvider(new ClusteredTravelTimeProvider(numPatterns))
			.in(Singleton.class);

		// Override TravelTime binding for all network modes that are analyzed
		for (String mode : networkModes) {
			if (analyzedModes.contains(mode)) {
				LOG.info("Binding clustered TravelTime for mode: {}", mode);
				addTravelTimeBinding(mode).toProvider(ClusteredTravelTimeForModeProvider.class).in(Singleton.class);
			}
		}

		// Optional: Add listener for periodic rebuilds during simulation
		if (rebuildIntervalSeconds > 0) {
			addMobsimListenerBinding().toProvider(PeriodicClusterRebuildListenerProvider.class);
		}
	}

	/**
	 * Provider for AdaptiveClusteredTravelTime.
	 */
	private static class ClusteredTravelTimeProvider implements Provider<AdaptiveClusteredTravelTime> {
		private final int numPatterns;

		@Inject private Network network;
		@Inject private Injector injector;
		@Inject private Config config;

		ClusteredTravelTimeProvider(int numPatterns) {
			this.numPatterns = numPatterns;
		}

		@Override
		public AdaptiveClusteredTravelTime get() {
			// Get the base TravelTime from the first analyzed mode's TravelTimeCalculator
			// This works because in "separate modes" config, each mode has its own calculator
			Set<String> analyzedModes = config.travelTimeCalculator().getAnalyzedModes();
			String firstMode = analyzedModes.iterator().next();

			TravelTime baseTravelTime;
			if (config.travelTimeCalculator().getSeparateModes()) {
				TravelTimeCalculator calculator = injector.getInstance(
					Key.get(TravelTimeCalculator.class, Names.named(firstMode)));
				baseTravelTime = calculator.getLinkTravelTimes();
			} else {
				TravelTimeCalculator calculator = injector.getInstance(TravelTimeCalculator.class);
				baseTravelTime = calculator.getLinkTravelTimes();
			}

			AdaptiveClusteredTravelTime.Config clusteredConfig = new AdaptiveClusteredTravelTime.Config()
				.setNumPatterns(numPatterns);

			return new AdaptiveClusteredTravelTime(network, baseTravelTime, clusteredConfig);
		}
	}

	/**
	 * Provider that returns the shared AdaptiveClusteredTravelTime for any mode.
	 */
	private static class ClusteredTravelTimeForModeProvider implements Provider<TravelTime> {
		@Inject private AdaptiveClusteredTravelTime clusteredTravelTime;

		@Override
		public TravelTime get() {
			return clusteredTravelTime;
		}
	}

	/**
	 * Provider for the periodic rebuild listener.
	 */
	private static class PeriodicClusterRebuildListenerProvider implements Provider<MobsimBeforeSimStepListener> {
		@Inject private AdaptiveClusteredTravelTime clusteredTravelTime;

		private final int intervalSeconds;

		PeriodicClusterRebuildListenerProvider() {
			this.intervalSeconds = 3600;
		}

		@Override
		public MobsimBeforeSimStepListener get() {
			return new PeriodicClusterRebuildListener(clusteredTravelTime, intervalSeconds);
		}
	}

	/**
	 * Listener that periodically rebuilds clusters during simulation.
	 */
	private static class PeriodicClusterRebuildListener implements MobsimBeforeSimStepListener {
		private final AdaptiveClusteredTravelTime clusteredTravelTime;
		private final int intervalSeconds;
		private double lastRebuildTime = Double.NEGATIVE_INFINITY;

		PeriodicClusterRebuildListener(AdaptiveClusteredTravelTime clusteredTravelTime, int intervalSeconds) {
			this.clusteredTravelTime = clusteredTravelTime;
			this.intervalSeconds = intervalSeconds;
		}

		@Override
		@SuppressWarnings("rawtypes")
		public void notifyMobsimBeforeSimStep(MobsimBeforeSimStepEvent e) {
			double simTime = e.getSimulationTime();
			if (simTime - lastRebuildTime >= intervalSeconds) {
				clusteredTravelTime.rebuildClusters();
				lastRebuildTime = simTime;
			}
		}
	}
}






