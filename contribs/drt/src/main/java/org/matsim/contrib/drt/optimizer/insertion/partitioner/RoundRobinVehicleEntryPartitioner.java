package org.matsim.contrib.drt.optimizer.insertion.partitioner;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.VehicleEntryPartitioner;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;

public class RoundRobinVehicleEntryPartitioner implements VehicleEntryPartitioner {

	@Override
	public List<Map<Id<DvrpVehicle>, VehicleEntry>> partition(
		Map<Id<DvrpVehicle>, VehicleEntry> entries, int n) {

		List<Map<Id<DvrpVehicle>, VehicleEntry>> partitions = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			partitions.add(new HashMap<>());
		}

		List<Map.Entry<Id<DvrpVehicle>, VehicleEntry>> sortedEntries = new ArrayList<>(entries.entrySet());
		sortedEntries.sort(Map.Entry.comparingByKey(Comparator.comparing(Id::toString)));

		int index = 0;
		for (Map.Entry<Id<DvrpVehicle>, VehicleEntry> entry : sortedEntries) {
			partitions.get(index % n).put(entry.getKey(), entry.getValue());
			index++;
		}

		return partitions;
	}
}

