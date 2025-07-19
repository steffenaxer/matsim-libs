package org.matsim.contrib.drt.extension.benchmark;

import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.RequestsPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.VehiclesPartitioner;

import java.util.List;

import static org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.RequestsPartitioner.*;
import static org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.VehiclesPartitioner.*;

public class DrtBenchmarkSuite {
    public static void main(String[] args) {
        BenchmarkRunner runner = new BenchmarkRunner("");

		// Param Combinations
        List<Integer> agentCounts = List.of(10000);
        List<String> insertionSearchList = List.of( "RepeatedSelectiveInsertionSearch");
        List<RequestsPartitioner> requestPartitioners = List.of(RoundRobinRequestsPartitioner, LoadAwareRoundRobinRequestsPartitioner);
        List<VehiclesPartitioner> vehiclePartitioners = List.of(ReplicatingVehicleEntryPartitioner, RoundRobinVehicleEntryPartitioner, ShiftingRoundRobinVehicleEntryPartitioner);
        List<Integer> collectionPeriods = List.of(30);
        List<Integer> workers = List.of(4);
        List<Integer> maxIters = List.of(1);
        List<Integer> threadsPerWorker = List.of(4);

        runner.runAll(agentCounts, insertionSearchList, requestPartitioners, vehiclePartitioners, collectionPeriods, workers, maxIters, threadsPerWorker);
    }
}
