package org.matsim.contrib.drt.extension.maintenance.scheduler;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.extension.edrt.schedule.EDrtChargingTask;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtMaintenanceTask;
import org.matsim.contrib.drt.extension.maintenance.tasks.EDrtMaintenanceTask;
import org.matsim.contrib.drt.extension.maintenance.tasks.MaintenanceTaskFactory;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.drt.scheduler.EmptyVehicleRelocator;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.path.VrpPathWithTravelData;
import org.matsim.contrib.dvrp.path.VrpPaths;
import org.matsim.contrib.dvrp.schedule.DriveTask;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.StayTask;
import org.matsim.contrib.dvrp.schedule.Task;
import org.matsim.contrib.dvrp.tracker.OnlineDriveTaskTracker;
import org.matsim.contrib.dvrp.util.LinkTimePair;
import org.matsim.contrib.evrp.ChargingTask;
import org.matsim.core.mobsim.framework.MobsimTimer;
import org.matsim.core.router.speedy.SpeedyALTFactory;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;

import static org.matsim.contrib.drt.extension.maintenance.dispatcher.MaintenanceTaskDispatcherImpl.MAINTENANCE_TIME;

/**
 * @author steffenaxer
 */
public class MaintenanceTaskSchedulerImpl implements MaintenanceTaskScheduler {
	private final Network network;
	private final TravelTime travelTime;
	private final MobsimTimer timer;
	private final MaintenanceTaskFactory taskFactory;
    private final LeastCostPathCalculator router;

	public MaintenanceTaskSchedulerImpl(Network network, TravelTime travelTime, TravelDisutility travelDisutility,
										MobsimTimer timer, DrtTaskFactory taskFactory) {
		this.network = network;
		this.travelTime = travelTime;
		this.timer = timer;
		this.taskFactory = (MaintenanceTaskFactory) taskFactory;
        this.router = new SpeedyALTFactory().createPathCalculator(network, travelDisutility, travelTime);
	}

	@Override
	public void scheduleMaintenanceTask(DvrpVehicle vehicle, OperationFacility maintenanceFacility) {
		final Schedule schedule = vehicle.getSchedule();

		final Task currentTask = schedule.getCurrentTask();

		if(currentTask instanceof EDrtChargingTask chargingTask)
		{
			double startTime = chargingTask.getEndTime();
			double endTime = startTime + MAINTENANCE_TIME;
			addMaintenanceTask(vehicle, startTime, endTime, chargingTask.getLink());
			return;
		}

		Link toLink = network.getLinks().get(maintenanceFacility.getLinkId());

		if (currentTask instanceof DriveTask
			&& currentTask.getTaskType().equals(EmptyVehicleRelocator.RELOCATE_VEHICLE_TASK_TYPE)
			&& currentTask.equals(schedule.getTasks().get(schedule.getTaskCount() - 2))) {
			//try to divert/cancel relocation
			LinkTimePair start = ((OnlineDriveTaskTracker) currentTask.getTaskTracker()).getDiversionPoint();

			VrpPathWithTravelData path;
			if (start != null) {
				toLink = network.getLinks().get(maintenanceFacility.getLinkId());
				path = VrpPaths.calcAndCreatePath(start.link, toLink, start.time, router, travelTime);
				((OnlineDriveTaskTracker) currentTask.getTaskTracker()).divertPath(path);

				// remove STAY
				schedule.removeLastTask();
			} else {
				start = new LinkTimePair(((DriveTask) currentTask).getPath().getToLink(), currentTask.getEndTime());
				path = VrpPaths.calcAndCreatePath(start.link, toLink, start.time, router, travelTime);

				// remove STAY
				schedule.removeLastTask();

				//add drive to maintenance location
				schedule.addTask(taskFactory.createDriveTask(vehicle, path, RELOCATE_MAINTENANCE_TASK_TYPE)); // add RELOCATE
			}

			double startTime = path.getArrivalTime();
			double endTime = startTime + MAINTENANCE_TIME;

			addMaintenanceTask(vehicle, startTime, endTime, toLink);

		} else {
			final Task task = schedule.getTasks().get(schedule.getTaskCount() - 1);
			final Link lastLink = ((StayTask) task).getLink();

			if (lastLink.getId() != maintenanceFacility.getLinkId()) {
				double departureTime = task.getBeginTime();

				if (schedule.getCurrentTask() == task) {
					departureTime = Math.max(task.getBeginTime(), timer.getTimeOfDay());
				}

				VrpPathWithTravelData path = VrpPaths.calcAndCreatePath(lastLink, toLink,
					departureTime, router,
					travelTime);
				if (path.getArrivalTime() < vehicle.getServiceEndTime()) {

					if (schedule.getCurrentTask() == task) {
						task.setEndTime(timer.getTimeOfDay());
					} else {
						// remove STAY
						schedule.removeLastTask();
					}

					//add drive to maintenance location
					schedule.addTask(taskFactory.createDriveTask(vehicle, path, RELOCATE_MAINTENANCE_TASK_TYPE)); // add RELOCATE
					double startTime = path.getArrivalTime();
					double endTime = startTime + MAINTENANCE_TIME;

					addMaintenanceTask(vehicle, startTime, endTime, toLink);
				}
			} else {
				double startTime;
				if (schedule.getCurrentTask() == task) {
					task.setEndTime(timer.getTimeOfDay());
					startTime = timer.getTimeOfDay();
				} else {
					// remove STAY
					startTime = task.getBeginTime();
					schedule.removeLastTask();
				}
				double endTime = startTime + MAINTENANCE_TIME;

				addMaintenanceTask(vehicle, startTime, endTime, toLink);
			}
		}
	}

	private void addMaintenanceTask(DvrpVehicle vehicle, double startTime, double endTime, Link link) {
		Schedule schedule = vehicle.getSchedule();

		// append DrtMaintenanceTask
		schedule.addTask(taskFactory.createMaintenanceTask(startTime, endTime, link));

		// append DrtStayTask
		schedule.addTask(taskFactory.createStayTask(vehicle, endTime, Math.max(vehicle.getServiceEndTime(),endTime) , link));
	}


}
