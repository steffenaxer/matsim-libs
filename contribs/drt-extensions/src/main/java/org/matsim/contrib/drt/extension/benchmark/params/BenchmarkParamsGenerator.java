package org.matsim.contrib.drt.extension.benchmark.params;

import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearch;
import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.RequestsPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.VehiclesPartitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BenchmarkParamsGenerator {
    public static List<BenchmarkParams> generateVariants(
			List<DrtInsertionSearchParams> insertionSearches,
            List<Integer> collectionPeriods,
            List<Integer> workers,
            List<Integer> maxIters,
            List<Integer> threadsPerWorker,
            List<RequestsPartitioner> requestPartitioners,
            List<VehiclesPartitioner> vehiclePartitioners,
			List<Integer> agentList
		) {

        List<BenchmarkParams> variants = new ArrayList<>();
		for (Integer agents : agentList) {
			for (DrtInsertionSearchParams insertionSearch : insertionSearches) {
				variants.add(new BenchmarkParams(agents, Optional.empty(), insertionSearch));
				for (RequestsPartitioner reqPart : requestPartitioners) {
					for (VehiclesPartitioner vehPart : vehiclePartitioners) {
						for (Integer period : collectionPeriods) {
							for (Integer worker : workers) {
								for (Integer thread : threadsPerWorker) {
									for (Integer iter : maxIters) {
										DrtParallelInserterParams drtParallelInserterParams = new DrtParallelInserterParams();
										drtParallelInserterParams.setCollectionPeriod(period);
										drtParallelInserterParams.setMaxIterations(iter);
										drtParallelInserterParams.setMaxPartitions(worker);
										drtParallelInserterParams.setMaxIterations(iter);
										drtParallelInserterParams.setInsertionSearchThreadsPerWorker(thread);
										drtParallelInserterParams.setRequestsPartitioner(reqPart);
										drtParallelInserterParams.setVehiclesPartitioner(vehPart);
										variants.add(new BenchmarkParams(agents, Optional.of(drtParallelInserterParams), insertionSearch));
									}
								}
							}
						}
					}
				}
			}
		}
        return variants;
    }
}
