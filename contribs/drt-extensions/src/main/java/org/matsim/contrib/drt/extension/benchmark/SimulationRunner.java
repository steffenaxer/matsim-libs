package org.matsim.contrib.drt.extension.benchmark;

import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.drt.optimizer.insertion.parallel.ParallelRequestInserterModule;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.run.DrtControlerCreator;
import org.matsim.core.config.Config;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy;

import java.nio.file.Path;

public class SimulationRunner {

    public static SimulationResult runBaseline(Scenario scenario) {
		Config config = scenario.getConfig();
        Controler controler = DrtControlerCreator.createControler(config, scenario, false);
        long start = System.nanoTime();
        controler.run();
        long end = System.nanoTime();

        double durationSeconds = (end - start) / 1e9;
        Path outputDir = Path.of(controler.getControlerIO().getOutputPath());

        return new SimulationResult(durationSeconds, outputDir);
    }

    public static SimulationResult runParallel( Scenario scenario, DrtConfigGroup drtConfigGroup) {
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
