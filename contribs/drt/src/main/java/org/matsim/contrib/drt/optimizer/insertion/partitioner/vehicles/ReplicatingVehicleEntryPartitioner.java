package org.matsim.contrib.drt.optimizer.insertion.partitioner.vehicles;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.RequestData;
import org.matsim.contrib.drt.optimizer.insertion.VehicleEntryPartitioner;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;

public class ReplicatingVehicleEntryPartitioner implements VehicleEntryPartitioner {

    @Override
    public List<Map<Id<DvrpVehicle>, VehicleEntry>> partition(
		Map<Id<DvrpVehicle>, VehicleEntry> entries, List<Collection<RequestData>> requestsPartitions) {
		int n = requestsPartitions.size();
        List<Map<Id<DvrpVehicle>, VehicleEntry>> partitions = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            partitions.add(new HashMap<>(entries));
        }

        return partitions;
    }
}
