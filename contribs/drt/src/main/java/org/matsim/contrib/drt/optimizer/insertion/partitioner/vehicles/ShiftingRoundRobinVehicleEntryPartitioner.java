package org.matsim.contrib.drt.optimizer.insertion.partitioner.vehicles;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.RequestData;
import org.matsim.contrib.drt.optimizer.insertion.VehicleEntryPartitioner;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ShiftingRoundRobinVehicleEntryPartitioner implements VehicleEntryPartitioner {

	private final AtomicInteger callCounter = new AtomicInteger(0);

	@Override
	public List<Map<Id<DvrpVehicle>, VehicleEntry>> partition(
		Map<Id<DvrpVehicle>, VehicleEntry> entries,
		List<Collection<RequestData>> requestsPartitions) {

		int n = requestsPartitions.size();
		List<Map<Id<DvrpVehicle>, VehicleEntry>> partitions = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			partitions.add(new HashMap<>());
		}

		List<Integer> activePartitionIndices = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			if (!requestsPartitions.get(i).isEmpty()) {
				activePartitionIndices.add(i);
			}
		}

		if (activePartitionIndices.isEmpty()) {
			return partitions;
		}

		List<Map.Entry<Id<DvrpVehicle>, VehicleEntry>> sortedEntries = new ArrayList<>(entries.entrySet());
		sortedEntries.sort(Map.Entry.comparingByKey(Comparator.comparing(Id::toString)));

		int shift = callCounter.getAndIncrement() % activePartitionIndices.size();

		int index = 0;
		for (Map.Entry<Id<DvrpVehicle>, VehicleEntry> entry : sortedEntries) {
			int partitionIndex = activePartitionIndices.get((index + shift) % activePartitionIndices.size());
			partitions.get(partitionIndex).put(entry.getKey(), entry.getValue());
			index++;
		}

		return partitions;
	}
}

