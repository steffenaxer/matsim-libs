package org.matsim.contrib.drt.optimizer.insertion;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface VehicleEntryPartitioner {
	List<Map<Id<DvrpVehicle>, VehicleEntry>> partition(
		Map<Id<DvrpVehicle>, VehicleEntry> entries, List<Collection<RequestData>> requestsPartitions);
}
