package org.matsim.contrib.drt.extension.maintenance.services.triggers;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public abstract class AbstractTrigger implements TriggerCounter, ServiceExecutionTrigger {
	int triggerCounter;
	Id<DvrpVehicle> vehicleId;

	AbstractTrigger(Id<DvrpVehicle> vehicleId)
	{
		this.vehicleId = vehicleId;
	}

	@Override
	public void incrementTrigger() {
		triggerCounter++;
	}

	@Override
	public int getTriggerCount() {
		return triggerCounter;
	}
}
