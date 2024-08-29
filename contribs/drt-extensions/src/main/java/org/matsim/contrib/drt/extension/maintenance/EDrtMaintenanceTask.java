package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.evrp.ETask;

/**
 * @author steffenaxer
 */
public class EDrtMaintenanceTask extends DrtMaintenanceTask implements ETask {

	public EDrtMaintenanceTask(double beginTime, double endTime, Link link, OperationFacility facility) {
		super(beginTime, endTime, link, facility);
	}

	@Override
	public double getTotalEnergy() {
		return 0;
	}
}

