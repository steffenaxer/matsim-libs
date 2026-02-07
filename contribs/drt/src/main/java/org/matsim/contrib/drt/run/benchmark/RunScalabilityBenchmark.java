/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** */

package org.matsim.contrib.drt.run.benchmark;

import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.ParallelRequestInserterModule;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.drt.run.benchmark.scenario.SyntheticBenchmarkScenario;
import org.matsim.core.controler.Controler;

import java.nio.file.Path;
import java.util.List;

/**
 * DRT Scalability Benchmark: Tests performance with varying agent counts.
 * <p>
 * Run: {@code java -cp matsim.jar org.matsim.contrib.drt.run.benchmark.RunScalabilityBenchmark}
 * <p>
 * <b>Determinism:</b> All modes are deterministic and produce reproducible results.
 * <ul>
 *   <li>Default: Deterministic (single-threaded)</li>
 *   <li>Parallel-Partitioning: Deterministic (static partitioning, sorted results)</li>
 *   <li>Parallel-LockingWorkStealing: Deterministic (deterministic partitioning with vehicle locking)</li>
 * </ul>
 *
 * @author Steffen Axer
 */
public class RunScalabilityBenchmark {

	public static void main(String[] args) {
		List<Integer> demandLevels = List.of(100_000); //100_000, 400_000);

		DrtBenchmarkRunner runner = DrtBenchmarkRunner.create()
			.warmupRuns(0)
			.measuredRuns(1)
			.reportTo(Path.of("output/benchmark/scalability_results_" + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".csv"));

		for (int agents : demandLevels) {
			int vehicles = agents / 100;
			String suffix = agents / 1000 + "k";

			// Default inserter
			runner.addScenario("Default-" + suffix, () -> {
				Controler c = SyntheticBenchmarkScenario.builder()
					.agents(agents).vehicles(vehicles)
					.outputDirectory("output/benchmark/default_" + suffix)
					.build();
				c.run();
			});

			// Parallel inserter - PARTITIONING mode (8 partitions)
			runner.addScenario("Parallel-Partitioning-" + suffix, () -> {
				Controler c = SyntheticBenchmarkScenario.builder()
					.agents(agents).vehicles(vehicles)
					.outputDirectory("output/benchmark/parallel_partitioning_" + suffix)
					.build();

				var drtCfg = MultiModeDrtConfigGroup.get(c.getConfig()).getModalElements().iterator().next();
				DrtParallelInserterParams params = new DrtParallelInserterParams();
				params.setMaxPartitions(8);
				params.setMaxIterations(3);
				params.setCollectionPeriod(120);
				params.setWorkDistributionMode(DrtParallelInserterParams.WorkDistributionMode.PARTITIONING);
				drtCfg.addParameterSet(params);
				c.addOverridingQSimModule(new ParallelRequestInserterModule(drtCfg));

				c.run();
			});

			// Parallel inserter - LOCKING_WORK_STEALING mode (4 workers)
			runner.addScenario("Parallel-LockingWorkStealing-" + suffix, () -> {
				Controler c = SyntheticBenchmarkScenario.builder()
					.agents(agents).vehicles(vehicles)
					.outputDirectory("output/benchmark/parallel_locking_" + suffix)
					.build();

				var drtCfg = MultiModeDrtConfigGroup.get(c.getConfig()).getModalElements().iterator().next();
				DrtParallelInserterParams params = new DrtParallelInserterParams();
				params.setMaxPartitions(4);
				params.setMaxIterations(1); // Usually only 1 iteration needed with locking
				params.setCollectionPeriod(120);
				params.setWorkDistributionMode(DrtParallelInserterParams.WorkDistributionMode.LOCKING_WORK_STEALING);
				params.setLockFailureStrategy(DrtParallelInserterParams.LockFailureStrategy.FIND_ALTERNATIVE);
				drtCfg.addParameterSet(params);
				c.addOverridingQSimModule(new ParallelRequestInserterModule(drtCfg));

				c.run();
			});
		}

		runner.run();
	}
}
