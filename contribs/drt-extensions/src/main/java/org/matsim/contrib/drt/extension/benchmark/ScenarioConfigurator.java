
package org.matsim.contrib.drt.extension.benchmark;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.common.zones.systems.grid.square.SquareGridZoneSystemParams;
import org.matsim.contrib.drt.extension.benchmark.scenario.FleetGenerator;
import org.matsim.contrib.drt.extension.benchmark.scenario.GridNetworkGenerator;
import org.matsim.contrib.drt.extension.benchmark.scenario.PopulationGenerator;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsParams;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsSetImpl;
import org.matsim.contrib.drt.optimizer.insertion.extensive.ExtensiveInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.repeatedselective.RepeatedSelectiveInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.selective.SelectiveInsertionSearchParams;
import org.matsim.contrib.drt.routing.DrtRoute;
import org.matsim.contrib.drt.routing.DrtRouteFactory;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.dvrp.run.DvrpConfigGroup;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.QSimConfigGroup;
import org.matsim.core.config.groups.ReplanningConfigGroup;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.controler.OutputDirectoryHierarchy;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.scenario.ScenarioUtils;

import java.nio.file.Path;

public class ScenarioConfigurator {
    private final double endTime;
    private final int expectedRidesPerVehicle;
    private final int iterations;

    public ScenarioConfigurator(double endTime, int expectedRidesPerVehicle, int iterations) {
        this.endTime = endTime;
        this.expectedRidesPerVehicle = expectedRidesPerVehicle;
        this.iterations = iterations;
    }

    public Scenario configureScenario(int numberOfAgents, String insertionSearch, String outputPath) {
		int numberOfVehicles = (int) (numberOfAgents / (endTime / 3600.) / expectedRidesPerVehicle);
        MatsimRandom.reset();
        Config config = ConfigUtils.createConfig();

		config.controller().setOutputDirectory(outputPath);
		config.controller().setOverwriteFileSetting(OutputDirectoryHierarchy.OverwriteFileSetting.deleteDirectoryIfExists);

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
        PopulationGenerator.generatePopulation(numberOfAgents, scenario, true);
        Path fleet = FleetGenerator.generateFleet(scenario, numberOfVehicles, 6, endTime, "fleet/fleet.xml");

        MultiModeDrtConfigGroup multiModeDrtConfigGroup = new MultiModeDrtConfigGroup();
        DrtConfigGroup drtConfig = new DrtConfigGroup();
        drtConfig.setVehiclesFile(fleet.toString());
        drtConfig.setStopDuration(30);

        switch (insertionSearch) {
            case ExtensiveInsertionSearchParams.SET_NAME:
                drtConfig.setDrtInsertionSearchParams(new ExtensiveInsertionSearchParams());
                break;
            case RepeatedSelectiveInsertionSearchParams.SET_NAME:
                drtConfig.setDrtInsertionSearchParams(new RepeatedSelectiveInsertionSearchParams());
                break;
            case SelectiveInsertionSearchParams.SET_NAME:
                drtConfig.setDrtInsertionSearchParams(new SelectiveInsertionSearchParams());
                break;
            default:
                throw new IllegalArgumentException("Unknown insertion search type: " + insertionSearch);
        }

        multiModeDrtConfigGroup.addDrtConfigGroup(drtConfig);

        DrtOptimizationConstraintsParams constraintsParams = drtConfig.addOrGetDrtOptimizationConstraintsParams();
        DrtOptimizationConstraintsSetImpl constraintsSet = constraintsParams.addOrGetDefaultDrtOptimizationConstraintsSet();
        constraintsSet.setMaxTravelTimeAlpha(2.);
        constraintsSet.setMaxTravelTimeBeta(600);
        constraintsSet.setMaxWaitTime(600);

        SquareGridZoneSystemParams zoneParams = new SquareGridZoneSystemParams();
        zoneParams.setCellSize(500);

        drtConfig.setNumberOfThreads(4);
        drtConfig.setOperationalScheme(DrtConfigGroup.OperationalScheme.door2door);
        config.addModule(multiModeDrtConfigGroup);

        ReplanningConfigGroup.StrategySettings strategy = new ReplanningConfigGroup.StrategySettings(Id.create("1", ReplanningConfigGroup.StrategySettings.class));
        strategy.setStrategyName("KeepLastSelected");
        strategy.setWeight(0.0);
        config.replanning().addStrategySettings(strategy);
        config.replanning().setMaxAgentPlanMemorySize(1);
        config.replanning().setFractionOfIterationsToDisableInnovation(0.0);

        ScoringConfigGroup.ModeParams drtParams = new ScoringConfigGroup.ModeParams("drt");
        drtParams.setMarginalUtilityOfTraveling(-6.0);
        drtParams.setConstant(0.0);
        config.scoring().addModeParams(drtParams);

        return scenario;
    }
}
