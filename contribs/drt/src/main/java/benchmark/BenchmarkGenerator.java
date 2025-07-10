package benchmark;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.contrib.common.zones.systems.grid.square.SquareGridZoneSystemParams;
import org.matsim.contrib.drt.optimizer.*;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsParams;
import org.matsim.contrib.drt.optimizer.constraints.DrtOptimizationConstraintsSetImpl;
import org.matsim.contrib.drt.optimizer.insertion.*;
import org.matsim.contrib.drt.optimizer.insertion.extensive.ExtensiveInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.ParallelUnplannedRequestInserter;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.requests.LoadAwareRoundRobinRequestsPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.requests.RequestsPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.vehicles.ShiftingRoundRobinVehicleEntryPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.partitioner.vehicles.VehicleEntryPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.repeatedselective.RepeatedSelectiveInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.selective.SelectiveInsertionSearchParams;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static org.matsim.contrib.drt.optimizer.insertion.parallel.ParallelInserterUtils.getDefaultPartitionScalingFunction;

public class BenchmarkGenerator {
	static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
	static String benchmarkTime = LocalDateTime.now().format(formatter);
	static NumberFormat usFormat = NumberFormat.getNumberInstance(Locale.US);
	static List<String[]> benchmarkResults = new ArrayList<>();
	// Scenario Setup
	static List<Integer> numberOfAgentsList = List.of(5000);
	static int expectedRidesPerVehicle = 7;
	static double endTime = 24 * 3600.;
	static int iterations = 1;


	// ParallelInsertion Setup
	static List<RequestsPartitioner> requestsPartitioners = List.of(new LoadAwareRoundRobinRequestsPartitioner(getDefaultPartitionScalingFunction()));
	static List<VehicleEntryPartitioner> vehicleEntryPartitioners = List.of(new ShiftingRoundRobinVehicleEntryPartitioner());
	static List<Integer> collectionPeriods = List.of(30);
	static List<Integer> workersList = List.of(1);
	static List<Integer> maxIterList = List.of(2);
	static List<Integer> insertionSearchThreadsPerWorkersList = List.of(4);

	public static Scenario configureScenario(int numberOfAgents) {
		int numberOfVehicles = (int) (numberOfAgents / (endTime / 3600.) / expectedRidesPerVehicle);
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
		DrtInsertionSearchParams insertionParams = new ExtensiveInsertionSearchParams();
		drtConfig.setDrtInsertionSearchParams(insertionParams);
		multiModeDrtConfigGroup.addDrtConfigGroup(drtConfig);

		DrtParallelInserterParams drtParallelInserterParams = new DrtParallelInserterParams();
		drtConfig.addParameterSet(drtParallelInserterParams);

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
		strategy.setWeight(0.0);
		config.replanning().addStrategySettings(strategy);

		config.replanning().setMaxAgentPlanMemorySize(1);
		config.replanning().setFractionOfIterationsToDisableInnovation(0.0);

		ScoringConfigGroup planCalcScore = config.scoring();
		ScoringConfigGroup.ModeParams drtParams = new ScoringConfigGroup.ModeParams("drt");
		drtParams.setMarginalUtilityOfTraveling(-6.0);
		drtParams.setConstant(0.0);
		planCalcScore.addModeParams(drtParams);

		return scenario;
	}

	public static void runBaseline(String uuid, int numberOfAgents) {
		Scenario scenario = configureScenario(numberOfAgents);
		Config config = scenario.getConfig();

		// Optional: set output directory and iterations
		config.controller().setOutputDirectory("output/" + uuid);
		config.controller().setOverwriteFileSetting(OutputDirectoryHierarchy.OverwriteFileSetting.deleteDirectoryIfExists);

		// Run the simulation
		Controler controler = DrtControlerCreator.createControler(scenario.getConfig(), scenario, false);
		long start = System.nanoTime();
		controler.run();
		long end = System.nanoTime();
		double durationSeconds = (end - start) / 1e9;

		Path outputDir = Path.of(controler.getControlerIO().getOutputPath());
		String rejectionRate = getValueFromCSV(outputDir.resolve("drt_customer_stats_drt.csv").toString(), "rejectionRate");
		String emptyRatio = getValueFromCSV(outputDir.resolve("drt_vehicle_stats_drt.csv").toString(), "emptyRatio");
		double requestDensityPerMinute = numberOfAgents / (24.0 * 60.);

		benchmarkResults.add(new String[]{
			numberOfAgents + "",
			"baseline",
			"baseline",
			"",
			"",
			"",
			"",
			usFormat.format(durationSeconds),
			rejectionRate,
			emptyRatio,
			requestDensityPerMinute + "",
			uuid,
		});

	}

	public static void writeBenchmark() {

		try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Path.of("benchmark_" + benchmarkTime + ".csv")))) {
			writer.println("agents;requestPartitioner;vehiclePartitioner;Threads;CollectionPeriod;MaxIter;InsertionSearchThreadsPerWorker;DurationSeconds;rejectionRate;emptyRatio;requestDensityPerMinute;uuid");
			for (String[] row : benchmarkResults) {
				writer.println(String.join(";", row));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void runParallelInserter(String uuid, int numberOfAgents, RequestsPartitioner requestsPartitioner, VehicleEntryPartitioner vehicleEntryPartitioner, int threads, double collectionPeriod, int maxIter, int insertionSearchThreadsPerWorker) {
		String reqPartName = requestsPartitioner.getClass().getSimpleName();
		String vehiclePartName = vehicleEntryPartitioner.getClass().getSimpleName();
		Scenario scenario = configureScenario(numberOfAgents);
		Config config = scenario.getConfig();

		// Optional: set output directory and iterations
		config.controller().setOutputDirectory("output/" + uuid);
		config.controller().setOverwriteFileSetting(OutputDirectoryHierarchy.OverwriteFileSetting.deleteDirectoryIfExists);

		var drtConfig = ConfigUtils.addOrGetModule(config, MultiModeDrtConfigGroup.class).getModalElements().iterator().next();

		// Run the simulation
		Controler controler = DrtControlerCreator.createControler(scenario.getConfig(), scenario, false);
		installParallelUnplannedRequestInserter(controler, drtConfig, requestsPartitioner, vehicleEntryPartitioner, threads, collectionPeriod, maxIter, insertionSearchThreadsPerWorker);

		long start = System.nanoTime();
		controler.run();
		long end = System.nanoTime();
		double durationSeconds = (end - start) / 1e9;

		Path outputDir = Path.of(controler.getControlerIO().getOutputPath());
		String rejectionRate = getValueFromCSV(outputDir.resolve("drt_customer_stats_drt.csv").toString(), "rejectionRate");
		String emptyRatio = getValueFromCSV(outputDir.resolve("drt_vehicle_stats_drt.csv").toString(), "emptyRatio");
		double requestDensityPerMinute = numberOfAgents / (24.0 * 60.);

		benchmarkResults.add(new String[]{
			numberOfAgents + "",
			reqPartName,
			vehiclePartName,
			String.valueOf(threads),
			String.valueOf(collectionPeriod),
			String.valueOf(maxIter),
			String.valueOf(insertionSearchThreadsPerWorker),
			usFormat.format(durationSeconds),
			rejectionRate,
			emptyRatio,
			requestDensityPerMinute + "",
			uuid
		});


	}

	public static void main(String[] args) {
		for (Integer numberOfAgents : numberOfAgentsList) {
			//runBaseline(UUID.randomUUID().toString(), numberOfAgents);
			for (RequestsPartitioner requestsPartitioner : requestsPartitioners) {
				for (VehicleEntryPartitioner vehicleEntryPartitioner : vehicleEntryPartitioners) {
					for (Integer collectionPeriod : collectionPeriods) {
						for (Integer worker : workersList) {
							for (Integer insertionSearchThreadsPerWorker : insertionSearchThreadsPerWorkersList) {
								for (Integer iter : maxIterList) {
									runParallelInserter(UUID.randomUUID().toString(), numberOfAgents, requestsPartitioner, vehicleEntryPartitioner, worker, collectionPeriod, iter, insertionSearchThreadsPerWorker);
									writeBenchmark();
								}
							}
						}
					}
				}
			}
		}
	}

	static void installParallelUnplannedRequestInserter(Controler controler, DrtConfigGroup drtConfigGroup, RequestsPartitioner requestsPartitioner, VehicleEntryPartitioner vehicleEntryPartitioner, int worker, double collectionPeriod, int maxIter, int insertionSearchThreadsPerWorker) {


	}

	public static String getValueFromCSV(String filePath, String field) {
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			String headerLine = reader.readLine();
			if (headerLine == null) return "N/A";

			String[] headers = headerLine.split(";");
			int rejectionRateIndex = -1;

			for (int i = 0; i < headers.length; i++) {
				if (headers[i].trim().equals(field)) {
					rejectionRateIndex = i;
					break;
				}
			}

			if (rejectionRateIndex == -1) return "N/A";

			String line;
			String lastLine = null;
			while ((line = reader.readLine()) != null) {
				lastLine = line;
			}

			if (lastLine != null) {
				String[] values = lastLine.split(";");
				if (rejectionRateIndex < values.length) {
					return values[rejectionRateIndex];
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "N/A";
	}




}
