package org.matsim.contrib.drt.extension.maintenance.tasks;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.evrp.ETask;

/**
 * @author steffenaxer
 */
public class EDrtMaintenanceTask extends DrtMaintenanceTask implements ETask {

	private final double consumedEnergy;

	public EDrtMaintenanceTask(double beginTime, double endTime, Link link, double consumedEnergy) {
		super(beginTime, endTime, link);
		this.consumedEnergy = consumedEnergy;
	}

	@Override
	public double getTotalEnergy() {
		return consumedEnergy;
	}
}

