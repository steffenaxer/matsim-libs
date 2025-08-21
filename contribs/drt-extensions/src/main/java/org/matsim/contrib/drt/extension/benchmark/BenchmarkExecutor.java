package org.matsim.contrib.drt.extension.benchmark;

import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.benchmark.params.BenchmarkParams;
import org.matsim.contrib.drt.extension.benchmark.utils.CSVUtils;
import org.matsim.contrib.drt.extension.insertion.spatialFilter.DrtSpatialRequestFleetFilterParams;
import org.matsim.contrib.drt.extension.insertion.spatialFilter.SpatialRequestFleetFilter;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.core.config.ConfigUtils;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class BenchmarkExecutor {
	private final String workDir;

	public BenchmarkExecutor(String workDir) {
		this.workDir = workDir;
	}

	private final BenchmarkRecorder recorder = new BenchmarkRecorder();

	public void runAll(List<BenchmarkParams> paramSets) {

		for (BenchmarkParams paramSet : paramSets) {
			String agents = String.valueOf(paramSet.agents);
			String reqPart = "N/A";
			String vehPart = "N/A";
			String worker = "N/A";
			String period = "N/A";
			String iter = "N/A";
			String thread = "N/A";


			String uuid = UUID.randomUUID().toString();
			ScenarioConfigurator scenarioConfigurator = new ScenarioConfigurator(3600 * 24, 3, 0);
			final Path path = Path.of(workDir);
			Scenario scenario = scenarioConfigurator.configureScenario(paramSet.agents, paramSet.drtInsertionSearch, path.resolve(uuid).toString());
			DrtWithExtensionsConfigGroup drtConfig = (DrtWithExtensionsConfigGroup) ConfigUtils.addOrGetModule(scenario.getConfig(), MultiModeDrtConfigGroup.class).getModalElements().iterator().next();

			if (paramSet.drtParallelInserterParams.isPresent()) {
				DrtParallelInserterParams p = paramSet.drtParallelInserterParams.get();
				reqPart = p.getRequestsPartitioner().name();
				vehPart = p.getVehiclesPartitioner().name();
				worker = String.valueOf(p.getMaxPartitions());
				period = String.valueOf(p.getCollectionPeriod());
				iter = String.valueOf(p.getMaxIterations());
				thread = String.valueOf(p.getInsertionSearchThreadsPerWorker());
				drtConfig.addParameterSet(paramSet.drtParallelInserterParams.get());
			}

			SimulationResult result = SimulationRunner.run(scenario, drtConfig);

			String rejectionRate = CSVUtils.getValueFromCSV(result.outputDir().resolve("drt_customer_stats_drt.csv").toString(), "rejectionRate").orElseThrow();
			String emptyRatio = CSVUtils.getValueFromCSV(result.outputDir().resolve("drt_vehicle_stats_drt.csv").toString(), "emptyRatio").orElseThrow();
			recorder.record(new String[]{
				agents, reqPart, vehPart, worker, period, iter , thread,
				String.format(Locale.US, "%.2f", result.durationSeconds()),
				rejectionRate, emptyRatio, uuid, paramSet.drtInsertionSearch.getName()
			});
			recorder.writeToFile(this.workDir);
		}
	}
}
