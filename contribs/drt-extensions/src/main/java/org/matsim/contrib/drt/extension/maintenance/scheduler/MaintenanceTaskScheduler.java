package org.matsim.contrib.drt.extension.maintenance.scheduler;

import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.schedule.DrtTaskType;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.DRIVE;

/**
 * @author steffenaxer
 */
public interface MaintenanceTaskScheduler {
	DrtTaskType RELOCATE_MAINTENANCE_TASK_TYPE = new DrtTaskType("RELOCATE_MAINTENANCE", DRIVE);
	void scheduleMaintenanceTask(DvrpVehicle vehicle, OperationFacility operationFacility);
}
