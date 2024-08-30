package org.matsim.contrib.drt.extension.maintenance.scheduler;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtMaintenanceTask;
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
	private final DrtTaskFactory taskFactory;
	private final LeastCostPathCalculator router;

	public MaintenanceTaskSchedulerImpl(Network network, TravelTime travelTime, TravelDisutility travelDisutility,
										MobsimTimer timer, DrtTaskFactory taskFactory) {
		this.network = network;
		this.travelTime = travelTime;
		this.timer = timer;
		this.taskFactory = taskFactory;
		this.router = new SpeedyALTFactory().createPathCalculator(network, travelDisutility, travelTime);
	}

	/**
	 * This code has been inspired by ShiftTaskSchedulerImpl
	 * TODO reduce code duplication and centralize code
	 */
	@Override
	public void relocateForMaintenance(DvrpVehicle vehicle, OperationFacility maintenanceFacility) {
		final Schedule schedule = vehicle.getSchedule();

		final Task currentTask = schedule.getCurrentTask();
		final Link toLink = network.getLinks().get(maintenanceFacility.getLinkId());

		if (currentTask instanceof DriveTask
			&& currentTask.getTaskType().equals(EmptyVehicleRelocator.RELOCATE_VEHICLE_TASK_TYPE)
			&& currentTask.equals(schedule.getTasks().get(schedule.getTaskCount() - 2))) {
			//try to divert/cancel relocation
			LinkTimePair start = ((OnlineDriveTaskTracker) currentTask.getTaskTracker()).getDiversionPoint();
			VrpPathWithTravelData path;
			if (start != null) {
				path = VrpPaths.calcAndCreatePath(start.link, toLink, start.time, router,
					travelTime);
				((OnlineDriveTaskTracker) currentTask.getTaskTracker()).divertPath(path);

				// remove STAY
				schedule.removeLastTask();
			} else {
				start = new LinkTimePair(((DriveTask) currentTask).getPath().getToLink(), currentTask.getEndTime());
				path = VrpPaths.calcAndCreatePath(start.link, toLink, start.time, router,
					travelTime);

				// remove STAY
				schedule.removeLastTask();

				//add drive to maintenance location
				schedule.addTask(taskFactory.createDriveTask(vehicle, path, RELOCATE_MAINTENANCE_TASK_TYPE)); // add RELOCATE
			}

			double startTime = path.getArrivalTime();
			double endTime = startTime + MAINTENANCE_TIME;

			relocateForMaintenanceImpl(vehicle, startTime, endTime, toLink, maintenanceFacility);

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

					relocateForMaintenanceImpl(vehicle, startTime, endTime, toLink, maintenanceFacility);
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

				relocateForMaintenanceImpl(vehicle, startTime, endTime, toLink, maintenanceFacility);
			}
		}
	}

	private void relocateForMaintenanceImpl(DvrpVehicle vehicle, double startTime, double endTime, Link link, OperationFacility maintenanceFacility) {
		Schedule schedule = vehicle.getSchedule();

		// append DrtMaintenanceTask
		schedule.addTask(new DrtMaintenanceTask(startTime, endTime, link, maintenanceFacility));

		// append DrtStayTask
		schedule.addTask(taskFactory.createStayTask(vehicle, endTime, Math.max(vehicle.getServiceEndTime(),endTime) , link));
	}

}
