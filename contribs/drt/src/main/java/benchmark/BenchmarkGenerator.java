package benchmark;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.common.zones.systems.grid.square.SquareGridZoneSystemParams;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsParams;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsSetImpl;
import org.matsim.contrib.drt.optimizer.insertion.*;
import org.matsim.contrib.drt.optimizer.insertion.partitioner.ReplicatingVehicleEntryPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.partitioner.RoundRobinVehicleEntryPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.partitioner.ShiftingRoundRobinVehicleEntryPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.repeatedselective.RepeatedSelectiveInsertionSearchParams;
import org.matsim.contrib.drt.passenger.DrtOfferAcceptor;
import org.matsim.contrib.drt.routing.DrtRoute;
import org.matsim.contrib.drt.routing.DrtRouteFactory;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.run.DrtControlerCreator;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.drt.scheduler.RequestInsertionScheduler;
import org.matsim.contrib.drt.stops.PassengerStopDurationProvider;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.run.DvrpConfigGroup;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.QSimConfigGroup;
import org.matsim.core.config.groups.ReplanningConfigGroup;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.mobsim.framework.MobsimTimer;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule;
import org.matsim.core.scenario.ScenarioUtils;

import java.nio.file.Path;
import java.util.List;

public class BenchmarkGenerator {
	static int numberOfAgents = 100000;
	static int expectedRidesPerVehicle = 7;
	static double endTime = 24 * 3600.;
	static int iterations = 2;
	static int numberOfVehicles = (int) (numberOfAgents / (endTime / 3600.) / expectedRidesPerVehicle);

	public static Scenario configureScenario() {
		MatsimRandom.reset();
		Config config = ConfigUtils.createConfig();

		config.controller().setWriteEventsInterval(0);
		config.controller().setWritePlansInterval(0);
		config.controller().setLastIteration(iterations);

		config.addModule(new DvrpConfigGroup());
		config.qsim().setSimStarttimeInterpretation(QSimConfigGroup.StarttimeInterpretation.onlyUseStarttime);
		config.global().setNumberOfThreads(4);

		config.qsim().setEndTime(endTime);
		config.qsim().setSimEndtimeInterpretation(QSimConfigGroup.EndtimeInterpretation.onlyUseEndtime);
		Scenario scenario = ScenarioUtils.createScenario(config);
		scenario.getPopulation().getFactory().getRouteFactories().setRouteFactory(DrtRoute.class, new DrtRouteFactory());

		GridNetworkGenerator.generateNetwork(scenario);

		PopulationGenerator.generatePopulation(numberOfAgents, scenario);
		Path fleet = FleetGenerator.generateFleet(scenario, numberOfVehicles, 6, endTime, "fleet/fleet.xml");

		MultiModeDrtConfigGroup multiModeDrtConfigGroup = new MultiModeDrtConfigGroup();
		DrtConfigGroup drtConfig = new DrtConfigGroup();
		drtConfig.setVehiclesFile(fleet.toString());
		drtConfig.setStopDuration(60);
		RepeatedSelectiveInsertionSearchParams insertionParams = new RepeatedSelectiveInsertionSearchParams();
		drtConfig.setDrtInsertionSearchParams(insertionParams);
		multiModeDrtConfigGroup.addDrtConfigGroup(drtConfig);

		DrtOptimizationConstraintsParams drtOptimizationConstraintsParams = drtConfig.addOrGetDrtOptimizationConstraintsParams();
		DrtOptimizationConstraintsSetImpl optimizationConstraintsSet = drtOptimizationConstraintsParams.addOrGetDefaultDrtOptimizationConstraintsSet();
		optimizationConstraintsSet.setMaxTravelTimeAlpha(2.);
		optimizationConstraintsSet.setMaxTravelTimeBeta(600);
		optimizationConstraintsSet.setMaxWaitTime(600);

		SquareGridZoneSystemParams squareGridZoneSystemParams = new SquareGridZoneSystemParams();
		squareGridZoneSystemParams.setCellSize(500);
		drtConfig.setNumberOfThreads(4); // Used for insertion search
		drtConfig.setOperationalScheme(DrtConfigGroup.OperationalScheme.door2door);
		config.addModule(multiModeDrtConfigGroup);

		ReplanningConfigGroup.StrategySettings strategy = new ReplanningConfigGroup.StrategySettings(Id.create("1", ReplanningConfigGroup.StrategySettings.class));
		strategy.setStrategyName(DefaultPlanStrategiesModule.DefaultSelector.KeepLastSelected);
		strategy.setWeight(1.0);
		config.replanning().addStrategySettings(strategy);

		ScoringConfigGroup planCalcScore = config.scoring();
		ScoringConfigGroup.ModeParams drtParams = new ScoringConfigGroup.ModeParams("drt");
		drtParams.setMarginalUtilityOfTraveling(-6.0);
		drtParams.setConstant(0.0);
		planCalcScore.addModeParams(drtParams);

		return scenario;
	}

	public static void runBaseline() {
		Scenario scenario = configureScenario();
		Config config = scenario.getConfig();

		// Optional: set output directory and iterations
		config.controller().setOutputDirectory("output/"+numberOfAgents+"_baseline");
		config.controller().setOverwriteFileSetting(OutputDirectoryHierarchy.OverwriteFileSetting.deleteDirectoryIfExists);

		// Run the simulation
		Controler controler = DrtControlerCreator.createControler(scenario.getConfig(), scenario, false);
		controler.run();
	}

	public static void runParallelInserter(VehicleEntryPartitioner vehicleEntryPartitioner, int threads, double collectionPeriod, int maxIter) {
		String partitioner = vehicleEntryPartitioner.getClass().getSimpleName();
		Scenario scenario = configureScenario();
		Config config = scenario.getConfig();

		// Optional: set output directory and iterations
		config.controller().setOutputDirectory("output/"+numberOfAgents+"_parallel-threads-" + threads + "-period-" + collectionPeriod + "-maxIter-" + maxIter +"-part-"+partitioner);
		config.controller().setOverwriteFileSetting(OutputDirectoryHierarchy.OverwriteFileSetting.deleteDirectoryIfExists);

		var drtConfig = ConfigUtils.addOrGetModule(config, MultiModeDrtConfigGroup.class).getModalElements().iterator().next();

		// Run the simulation
		Controler controler = DrtControlerCreator.createControler(scenario.getConfig(), scenario, false);
		installParallelUnplannedRequestInserter(controler, drtConfig,vehicleEntryPartitioner , threads, collectionPeriod, maxIter);
		controler.run();
	}

	public static void main(String[] args) {
		//runBaseline();

		var partitioner = List.of(new ShiftingRoundRobinVehicleEntryPartitioner());
		var collectionPeriods = List.of(30);
		var threads = List.of(4);
		int maxIter = 3;

		for (VehicleEntryPartitioner vehicleEntryPartitioner : partitioner) {
			for (Integer collectionPeriod : collectionPeriods) {
				for (Integer thread : threads) {
					runParallelInserter(vehicleEntryPartitioner, thread, collectionPeriod, maxIter);
				}
			}
		}

	}

	static void installParallelUnplannedRequestInserter(Controler controler, DrtConfigGroup drtConfigGroup, VehicleEntryPartitioner vehicleEntryPartitioner, int threads, double collectionPeriod, int maxIter) {

		controler.addOverridingQSimModule(new AbstractDvrpModeQSimModule(drtConfigGroup.getMode()) {
			@Override
			protected void configureQSim() {
				bindModal(UnplannedRequestInserter.class).toProvider(modalProvider(
					getter -> new ParallelUnplannedRequestInserter(vehicleEntryPartitioner, threads, collectionPeriod, maxIter, drtConfigGroup, getter.getModal(Fleet.class),
						getter.get(MobsimTimer.class), getter.get(EventsManager.class),
						() -> getter.getModal(RequestInsertionScheduler.class),
						getter.getModal(VehicleEntry.EntryFactory.class),
						() -> getter.getModal(DrtInsertionSearch.class),
						getter.getModal(DrtRequestInsertionRetryQueue.class),
						getter.getModal(DrtOfferAcceptor.class),
						getter.getModal(PassengerStopDurationProvider.class),
						getter.getModal(RequestFleetFilter.class)
					))).asEagerSingleton();
			}
		});
	}
}
