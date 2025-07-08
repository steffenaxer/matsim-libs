package org.matsim.contrib.drt.optimizer.insertion.partitioner.requests;

import org.matsim.contrib.drt.optimizer.insertion.RequestData;
import org.matsim.contrib.drt.passenger.DrtRequest;

import java.util.*;

public class RoundRobinRequestsPartitioner implements RequestsPartitioner {
	private long counter = 0;

    @Override
    public List<Collection<RequestData>> partition(Collection<DrtRequest> unplannedRequests, int n, double collectionPeriod) {
        List<Collection<RequestData>> partitions = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            partitions.add(new ArrayList<>());
        }

        Iterator<DrtRequest> it = unplannedRequests.iterator();
        while (it.hasNext()) {
            DrtRequest request = it.next();
            int partitionIndex = (int) counter % n;
            partitions.get(partitionIndex).add(new RequestData(request));
            it.remove();
            counter++;
        }

        return partitions;
    }
}
