package org.matsim.contrib.drt.benchmark;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsParams;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsSetImpl;
import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.extensive.ExtensiveInsertionSearchParams;
import org.matsim.contrib.drt.routing.DrtRoute;
import org.matsim.contrib.drt.routing.DrtRouteFactory;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.run.DrtControlerCreator;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.dvrp.run.DvrpConfigGroup;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.QSimConfigGroup;
import org.matsim.core.config.groups.ReplanningConfigGroup;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy;
import org.matsim.core.scenario.ScenarioUtils;

public class BenchmarkGenerator {
	public static void main(String[] args) {
		Config config = ConfigUtils.createConfig();

		config.addModule(new DvrpConfigGroup());
		config.qsim().setSimStarttimeInterpretation(QSimConfigGroup.StarttimeInterpretation.onlyUseStarttime);
		config.qsim().setEndTime(24*3600.);
		config.qsim().setSimEndtimeInterpretation(QSimConfigGroup.EndtimeInterpretation.onlyUseEndtime);
		Scenario scenario = ScenarioUtils.createScenario(config);
		scenario.getPopulation().getFactory().getRouteFactories().setRouteFactory(DrtRoute.class, new DrtRouteFactory());

		GridNetworkGenerator.generateNetwork(scenario);
		PopulationGenerator.generatePopulation(5000, scenario);

		MultiModeDrtConfigGroup multiModeDrtConfigGroup = new MultiModeDrtConfigGroup();
		DrtConfigGroup drtConfig = new DrtConfigGroup();
		drtConfig.setStopDuration(60);
		ExtensiveInsertionSearchParams insertionParams = new ExtensiveInsertionSearchParams();
		drtConfig.setDrtInsertionSearchParams(insertionParams);
		multiModeDrtConfigGroup.addDrtConfigGroup(drtConfig);

		DrtOptimizationConstraintsParams drtOptimizationConstraintsParams = drtConfig.addOrGetDrtOptimizationConstraintsParams();
		DrtOptimizationConstraintsSetImpl optimizationConstraintsSet = drtOptimizationConstraintsParams.addOrGetDefaultDrtOptimizationConstraintsSet();
		optimizationConstraintsSet.setMaxTravelTimeAlpha(2.);
		optimizationConstraintsSet.setMaxTravelTimeBeta(600);
		optimizationConstraintsSet.setMaxWaitTime(600);

		drtConfig.setOperationalScheme(DrtConfigGroup.OperationalScheme.door2door);

		config.addModule(multiModeDrtConfigGroup);

		// Optional: set output directory and iterations
		config.controller().setOutputDirectory("output/drt-scenario");
		config.controller().setLastIteration(10);
		config.controller().setOverwriteFileSetting(OutputDirectoryHierarchy.OverwriteFileSetting.deleteDirectoryIfExists);


		ReplanningConfigGroup.StrategySettings strategy = new ReplanningConfigGroup.StrategySettings(Id.create("1", ReplanningConfigGroup.StrategySettings.class));
		strategy.setStrategyName("ChangeExpBeta");
		strategy.setWeight(1.0);
		config.replanning().addStrategySettings(strategy);


		// Run the simulation
		Controler controler = DrtControlerCreator.createControler(scenario.getConfig(), scenario, false);
		controler.run();

	}
}
