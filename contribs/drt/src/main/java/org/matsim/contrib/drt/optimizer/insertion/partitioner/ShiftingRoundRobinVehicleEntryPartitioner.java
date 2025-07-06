package org.matsim.contrib.drt.optimizer.insertion.partitioner;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.optimizer.insertion.VehicleEntryPartitioner;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ShiftingRoundRobinVehicleEntryPartitioner implements VehicleEntryPartitioner {

    private final AtomicInteger callCounter = new AtomicInteger(0);

    @Override
    public List<Map<Id<DvrpVehicle>, VehicleEntry>> partition(
            Map<Id<DvrpVehicle>, VehicleEntry> entries, int n) {

        List<Map<Id<DvrpVehicle>, VehicleEntry>> partitions = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            partitions.add(new HashMap<>());
        }

        List<Map.Entry<Id<DvrpVehicle>, VehicleEntry>> sortedEntries = new ArrayList<>(entries.entrySet());
        sortedEntries.sort(Map.Entry.comparingByKey(Comparator.comparing(Id::toString)));

        int shift = callCounter.getAndIncrement() % n;

        int index = 0;
        for (Map.Entry<Id<DvrpVehicle>, VehicleEntry> entry : sortedEntries) {
            int partitionIndex = (index + shift) % n;
            partitions.get(partitionIndex).put(entry.getKey(), entry.getValue());
            index++;
        }

        return partitions;
    }
}
