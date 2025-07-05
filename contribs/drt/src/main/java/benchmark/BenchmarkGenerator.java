package benchmark;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.common.zones.ZoneSystemParams;
import org.matsim.contrib.common.zones.systems.grid.square.SquareGridZoneSystemParams;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.QSimScopeForkJoinPoolHolder;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsParams;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsSetImpl;
import org.matsim.contrib.drt.optimizer.insertion.*;
import org.matsim.contrib.drt.optimizer.insertion.extensive.ExtensiveInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.repeatedselective.RepeatedSelectiveInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.selective.SelectiveInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.rebalancing.RebalancingParams;
import org.matsim.contrib.drt.optimizer.rebalancing.mincostflow.MinCostFlowRebalancingStrategyParams;
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
import org.matsim.core.mobsim.framework.MobsimTimer;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule;
import org.matsim.core.scenario.ScenarioUtils;

import java.nio.file.Path;

public class BenchmarkGenerator {
	public static void main(String[] args) {

		int numberOfAgents = 500000;
		int expectedRidesPerVehicle = 3;
		double endTime = 24 * 3600.;
		int iterations = 0;
		int numberOfVehicles = (int) (numberOfAgents / (endTime/3600.) /expectedRidesPerVehicle);

		Config config = ConfigUtils.createConfig();

		config.addModule(new DvrpConfigGroup());
		config.qsim().setSimStarttimeInterpretation(QSimConfigGroup.StarttimeInterpretation.onlyUseStarttime);


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
		RebalancingParams rebalancingParams = new RebalancingParams();
		rebalancingParams.addParameterSet(squareGridZoneSystemParams);
		MinCostFlowRebalancingStrategyParams minCostFlowRebalancingStrategyParams = new MinCostFlowRebalancingStrategyParams();
		minCostFlowRebalancingStrategyParams.setTargetAlpha(0.3);
		minCostFlowRebalancingStrategyParams.setTargetBeta(0.5);
		rebalancingParams.addParameterSet(minCostFlowRebalancingStrategyParams);
		drtConfig.addParameterSet(rebalancingParams);


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


		// Run the simulation
		Controler controler = DrtControlerCreator.createControler(scenario.getConfig(), scenario, false);

		// Optional: set output directory and iterations
		int threads = 4;
		config.controller().setOutputDirectory("output/drt-scenario-parallel-"+threads);
		config.controller().setLastIteration(iterations);
		config.controller().setOverwriteFileSetting(OutputDirectoryHierarchy.OverwriteFileSetting.deleteDirectoryIfExists);
		installParallelUnplannedRequestInserter(controler,drtConfig, threads);

		controler.run();

	}

	static void installParallelUnplannedRequestInserter(Controler controler, DrtConfigGroup drtConfigGroup, int threads) {

		controler.addOverridingQSimModule(new AbstractDvrpModeQSimModule(drtConfigGroup.getMode()) {
			@Override
			protected void configureQSim() {
				bindModal(UnplannedRequestInserter.class).toProvider(modalProvider(
						getter -> new ParallelUnplannedRequestInserter(threads, drtConfigGroup, getter.getModal(Fleet.class),
								getter.get(MobsimTimer.class), getter.get(EventsManager.class),
								() -> getter.getModal(RequestInsertionScheduler.class),
								getter.getModal(VehicleEntry.EntryFactory.class),
								() -> getter.getModal(DrtInsertionSearch.class),
								getter.getModal(DrtRequestInsertionRetryQueue.class),
								getter.getModal(DrtOfferAcceptor.class),
								getter.getModal(PassengerStopDurationProvider.class),
								getter.getModal(RequestFleetFilter.class),
								getter.getModal(Network.class)))).asEagerSingleton();
			}
		});
	}
}
