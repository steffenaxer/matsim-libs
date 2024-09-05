package org.matsim.contrib.drt.extension.maintenance.services.triggers;

import org.apache.commons.lang3.mutable.MutableInt;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServiceParams;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import java.util.*;

/**
 * @author steffenaxer
 */
public class ServiceExecutionTracker
{
	private final Id<DvrpVehicle> vehicleId;
	Map<DrtServiceParams, List<ServiceExecutionTrigger>> service2Triggers = new HashMap<>();
	Map<DrtServiceParams, MutableInt> executionCounter = new HashMap<>();

	public ServiceExecutionTracker(Id<DvrpVehicle> vehicleId)
	{
		this.vehicleId = vehicleId;
	}

	public Set<DrtServiceParams> getServices()
	{
		return this.service2Triggers.keySet();
	}

	public void addTrigger(DrtServiceParams drtServiceParams, ServiceExecutionTrigger trigger)
	{
		this.service2Triggers.computeIfAbsent(drtServiceParams, k -> new ArrayList<>()).add(trigger);
	}

	public List<ServiceExecutionTrigger> getTriggers(DrtServiceParams drtServiceParams)
	{
		return this.service2Triggers.get(drtServiceParams);
	}

	public Id<DvrpVehicle> getVehicleId() {
		return vehicleId;
	}

	public void incrementExecutionCounter(DrtServiceParams drtServiceParams) {
		this.executionCounter.computeIfAbsent(drtServiceParams, k -> new MutableInt(0)).increment();
	}

	public int getTriggerCount(DrtServiceParams drtServiceParams) {
		return this.executionCounter.computeIfAbsent(drtServiceParams, k -> new MutableInt(0)).getValue();
	}
}
