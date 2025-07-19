package org.matsim.contrib.drt.extension.benchmark.params;

import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearch;
import org.matsim.contrib.drt.optimizer.insertion.DrtInsertionSearchParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.RequestsPartitioner;
import org.matsim.contrib.drt.optimizer.insertion.parallel.DrtParallelInserterParams.VehiclesPartitioner;

import java.util.Optional;

public class BenchmarkParams {
	public final int agents;
	public final Optional<DrtParallelInserterParams> drtParallelInserterParams;
	public final DrtInsertionSearchParams drtInsertionSearch;

    public BenchmarkParams(int agents, Optional<DrtParallelInserterParams> drtParallelInserterParams, DrtInsertionSearchParams drtInsertionSearchParams)
	{
		this.agents = agents;
		this.drtParallelInserterParams = drtParallelInserterParams;
		this.drtInsertionSearch = drtInsertionSearchParams;
	}
}
