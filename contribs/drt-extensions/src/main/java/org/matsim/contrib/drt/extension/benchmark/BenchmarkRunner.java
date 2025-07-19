package org.matsim.contrib.drt.extension.benchmark;

import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.RequestsPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.VehiclesPartitioner;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.core.config.ConfigUtils;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class BenchmarkRunner {
	private final String workDir;

	public BenchmarkRunner(String workDir)
	{
		this.workDir = workDir;
	}

    private final BenchmarkRecorder recorder = new BenchmarkRecorder();

    public void runAll(List<Integer> agentCounts, List<String> insertionSearchList,
                       List<RequestsPartitioner> requestPartitioners,
                       List<VehiclesPartitioner> vehiclePartitioners,
                       List<Integer> collectionPeriods, List<Integer> workers,
                       List<Integer> maxIters, List<Integer> threadsPerWorker) {

		ScenarioConfigurator scenarioConfigurator = new ScenarioConfigurator( 3600 * 24, 5,  2);
		final Path path = Path.of(workDir);
        for (String insertionSearch : insertionSearchList) {
            for (Integer agents : agentCounts) {
                String uuid = UUID.randomUUID().toString();
				Scenario scenario = scenarioConfigurator.configureScenario(agents, insertionSearch, path.resolve(uuid).toString());
                SimulationResult result = SimulationRunner.runBaseline(scenario);
                String rejectionRate = CSVUtils.getValueFromCSV(result.outputDir().resolve("drt_customer_stats_drt.csv").toString(), "rejectionRate").orElseThrow();
                String emptyRatio = CSVUtils.getValueFromCSV(result.outputDir().resolve("drt_vehicle_stats_drt.csv").toString(), "emptyRatio").orElseThrow();
                recorder.record(new String[]{
                        agents + "", "baseline", "baseline", "N/A", "N/A", "N/A", "N/A",
                        String.format(Locale.US, "%.2f", result.durationSeconds()),
                        rejectionRate, emptyRatio, uuid, insertionSearch
                });
				recorder.writeToFile(this.workDir);

                for (RequestsPartitioner reqPart : requestPartitioners) {
                    for (VehiclesPartitioner vehPart : vehiclePartitioners) {
                        for (Integer period : collectionPeriods) {
                            for (Integer worker : workers) {
                                for (Integer thread : threadsPerWorker) {
                                    for (Integer iter : maxIters) {
                                        String uuid2 = UUID.randomUUID().toString();
                                        Scenario scenario2 = scenarioConfigurator.configureScenario(agents, insertionSearch, path.resolve(uuid).toString());
                                        DrtConfigGroup drtConfig = ConfigUtils.addOrGetModule(scenario2.getConfig(), MultiModeDrtConfigGroup.class).getModalElements().iterator().next();
                                        DrtParallelInserterParams params = new DrtParallelInserterParams();
                                        params.setCollectionPeriod(period);
                                        params.setMaxPartitions(worker);
                                        params.setMaxIterations(iter);
                                        params.setInsertionSearchThreadsPerWorker(thread);
                                        params.setVehiclesPartitioner(vehPart);
                                        params.setRequestsPartitioner(reqPart);
                                        params.setLogThreadActivity(true);
                                        drtConfig.addParameterSet(params);

                                        SimulationResult result2 = SimulationRunner.runParallel(scenario2, drtConfig);

                                        String rejectionRate2 = CSVUtils.getValueFromCSV(result2.outputDir().resolve("drt_customer_stats_drt.csv").toString(), "rejectionRate").orElseThrow();
                                        String emptyRatio2 = CSVUtils.getValueFromCSV(result2.outputDir().resolve("drt_vehicle_stats_drt.csv").toString(), "emptyRatio").orElseThrow();
                                        recorder.record(new String[]{
                                                agents + "", reqPart.name(), vehPart.name(), worker + "", period + "", iter + "", thread + "",
                                                String.format(Locale.US, "%.2f", result2.durationSeconds()),
                                                rejectionRate2, emptyRatio2, uuid2, insertionSearch

                                        });
										recorder.writeToFile(this.workDir);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
