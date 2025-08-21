package org.matsim.contrib.drt.extension.benchmark;

import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.drt.extension.insertion.spatialFilter.SpatialFilterInsertionSearchQSimModule;
import org.matsim.contrib.drt.optimizer.insertion.parallel.ParallelRequestInserterModule;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.run.DrtControlerCreator;
import org.matsim.core.config.Config;
import org.matsim.core.controler.Controler;

import java.nio.file.Path;

public class SimulationRunner {


    public static SimulationResult run(Scenario scenario, DrtConfigGroup drtConfigGroup) {
		Config config = scenario.getConfig();
		Controler controller = DrtControlerCreator.createControler(config, scenario, false);
		controller.addOverridingQSimModule(new ParallelRequestInserterModule(drtConfigGroup));
        long start = System.nanoTime();
		controller.run();
        long end = System.nanoTime();

        double durationSeconds = (end - start) / 1e9;
        Path outputDir = Path.of(controller.getControlerIO().getOutputPath());

        return new SimulationResult(durationSeconds, outputDir);
    }
}
