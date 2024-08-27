package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.extension.operations.shifts.fleet.ShiftDvrpVehicle;
import org.matsim.contrib.drt.extension.operations.shifts.shift.DrtShift;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

public interface MaintenanceScheduler {
	void scheduleMaintenanceTask(DvrpVehicle vehicle, OperationFacility breakFacility, DrtShift shift);
}
