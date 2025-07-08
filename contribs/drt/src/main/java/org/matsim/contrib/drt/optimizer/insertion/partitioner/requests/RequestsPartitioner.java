package org.matsim.contrib.drt.optimizer.insertion.partitioner.requests;

import org.matsim.contrib.drt.optimizer.insertion.RequestData;
import org.matsim.contrib.drt.passenger.DrtRequest;

import java.util.Collection;
import java.util.List;

public interface RequestsPartitioner {
	List<Collection<RequestData>> partition(Collection<DrtRequest> unplannedRequests, int n, double collectionPeriod);
}
