package org.matsim.contrib.drt.extension.maintenance.dispatcher;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.maintenance.logic.MaintenanceRule;
import org.matsim.contrib.drt.extension.maintenance.logic.MaintenanceRuleCollectorImpl;
import org.matsim.contrib.drt.extension.maintenance.scheduler.MaintenanceTaskScheduler;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilityFinder;
import org.matsim.contrib.drt.schedule.DrtStayTask;
import org.matsim.contrib.drt.scheduler.EmptyVehicleRelocator;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.schedule.DriveTask;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;
import org.matsim.contrib.dvrp.tracker.OnlineDriveTaskTracker;
import org.matsim.contrib.dvrp.util.LinkTimePair;
import org.matsim.core.api.experimental.events.EventsManager;

/**
 * @author steffenaxer
 */
public class MaintenanceTaskDispatcherImpl implements MaintenanceTaskDispatcher {
	public static final double MAINTENANCE_TIME = 900.;
	private final Fleet fleet;
	private final EventsManager eventsManager;
	private final MaintenanceTaskScheduler maintenanceTaskScheduler;
	private final MaintenanceRuleCollectorImpl maintenanceRuleCollectorImpl;
	private final OperationFacilityFinder operationFacilityFinder;

	public MaintenanceTaskDispatcherImpl(Fleet fleet, EventsManager eventsManager,
										 MaintenanceTaskScheduler maintenanceTaskScheduler,
										 MaintenanceRuleCollectorImpl maintenanceRuleCollectorImpl,
										 OperationFacilityFinder operationFacilityFinder) {
		this.fleet = fleet;
		this.eventsManager = eventsManager;
		this.maintenanceTaskScheduler = maintenanceTaskScheduler;
		this.maintenanceRuleCollectorImpl = maintenanceRuleCollectorImpl;
		this.operationFacilityFinder = operationFacilityFinder;
	}

	@Override
	public void dispatch(double timeStep) {
		for (DvrpVehicle dvrpVehicle : this.fleet.getVehicles().values()) {
			for (MaintenanceRule maintenanceRule : this.maintenanceRuleCollectorImpl.getMaintenanceRules())
			{
				if(maintenanceRule.requiresMaintenance(dvrpVehicle,timeStep))
				{
					this.maintenanceTaskScheduler.scheduleMaintenanceTask(dvrpVehicle, findMaintenanceFacility(dvrpVehicle));
					break; // Only assign one maintenance task
				}
			}
		}
	}

	private OperationFacility findMaintenanceFacility(DvrpVehicle dvrpVehicle) {
		final Schedule schedule = dvrpVehicle.getSchedule();
		Task currentTask = schedule.getCurrentTask();
		Link lastLink;
		if (currentTask instanceof DriveTask
			&& currentTask.getTaskType().equals(EmptyVehicleRelocator.RELOCATE_VEHICLE_TASK_TYPE)
			&& currentTask.equals(schedule.getTasks().get(schedule.getTaskCount()-2))) {
			LinkTimePair start = ((OnlineDriveTaskTracker) currentTask.getTaskTracker()).getDiversionPoint();
			if(start != null) {
				lastLink = start.link;
			} else {
				lastLink = ((DriveTask) currentTask).getPath().getToLink();
			}
		}  else {
			lastLink = ((DrtStayTask) schedule.getTasks()
				.get(schedule.getTaskCount() - 1)).getLink();
		}
		return operationFacilityFinder.findFacility(lastLink.getCoord()).orElseThrow();
	}

}
