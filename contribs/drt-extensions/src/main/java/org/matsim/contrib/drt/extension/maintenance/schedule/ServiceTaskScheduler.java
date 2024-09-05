package org.matsim.contrib.drt.extension.maintenance.schedule;

import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServiceParams;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.schedule.DrtTaskType;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.DRIVE;

/**
 * @author steffenaxer
 */
public interface ServiceTaskScheduler {
	DrtTaskType RELOCATE_SERVICE_TASK_TYPE = new DrtTaskType("RELOCATE_SERVICE", DRIVE);
	void scheduleServiceTask(DvrpVehicle vehicle, OperationFacility operationFacility, DrtServiceParams drtServiceParams);
}
