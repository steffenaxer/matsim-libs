package org.matsim.contrib.drt.extension.benchmark;

import org.matsim.contrib.drt.extension.benchmark.params.BenchmarkParamsGenerator;
import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.RequestsPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.VehiclesPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.repeatedselective.RepeatedSelectiveInsertionSearchParams;

import java.util.List;

import static org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.RequestsPartitioner.*;
import static org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.VehiclesPartitioner.*;

public class DrtBenchmarkSuite {
	public static void main(String[] args) {
		BenchmarkExecutor benchmarkExecutor = new BenchmarkExecutor("C:\\tmp");

		// Param Combinations
		List<Integer> agentCounts = List.of(200_000);
		List<DrtInsertionSearchParams> insertionSearchList = List.of(new RepeatedSelectiveInsertionSearchParams());
		List<RequestsPartitioner> requestPartitioners = List.of(LoadAwareRoundRobinRequestsPartitioner);
		List<VehiclesPartitioner> vehiclePartitioners = List.of(ShiftingRoundRobinVehicleEntryPartitioner);
		List<Integer> collectionPeriods = List.of(15);
		List<Integer> workers = List.of(10);
		List<Integer> maxIters = List.of(2);
		List<Integer> threadsPerWorker = List.of(4);

		var params = BenchmarkParamsGenerator.generateVariants(true, insertionSearchList,
			collectionPeriods,
			workers,
			maxIters,
			threadsPerWorker,
			requestPartitioners,
			vehiclePartitioners,
			agentCounts);

		benchmarkExecutor.runAll(params);
	}
}
