package org.matsim.contrib.drt.extension.maintenance.schedule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.extension.edrt.schedule.EDrtChargingTask;
import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServiceParams;
import org.matsim.contrib.drt.extension.maintenance.tasks.DefaultJoinableTasksImpl;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtServiceTask;
import org.matsim.contrib.drt.extension.maintenance.tasks.ServiceTaskFactory;
import org.matsim.contrib.drt.extension.maintenance.tasks.JoinableTasks;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.drt.scheduler.EmptyVehicleRelocator;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.path.VrpPathWithTravelData;
import org.matsim.contrib.dvrp.path.VrpPaths;
import org.matsim.contrib.dvrp.schedule.*;
import org.matsim.contrib.dvrp.tracker.OnlineDriveTaskTracker;
import org.matsim.contrib.dvrp.util.LinkTimePair;
import org.matsim.core.mobsim.framework.MobsimTimer;
import org.matsim.core.router.speedy.SpeedyALTFactory;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.TravelDisutility;
import org.matsim.core.router.util.TravelTime;

/**
 * @author steffenaxer
 */
public class ServiceTaskSchedulerImpl implements ServiceTaskScheduler {
	private static final Logger LOG = LogManager.getLogger(ServiceTaskSchedulerImpl.class);
	private final static JoinableTasks JOINABLE_SERVICES = new DefaultJoinableTasksImpl();
	private final Network network;
	private final TravelTime travelTime;
	private final MobsimTimer timer;
	private final ServiceTaskFactory taskFactory;
	private final LeastCostPathCalculator router;

	public ServiceTaskSchedulerImpl(Network network, TravelTime travelTime, TravelDisutility travelDisutility,
									MobsimTimer timer, DrtTaskFactory taskFactory) {
		this.network = network;
		this.travelTime = travelTime;
		this.timer = timer;
		this.taskFactory = (ServiceTaskFactory) taskFactory;
		this.router = new SpeedyALTFactory().createPathCalculator(network, travelDisutility, travelTime);
	}

	@Override
	public void scheduleServiceTask(DvrpVehicle vehicle, OperationFacility maintenanceFacility, DrtServiceParams drtServiceParams) {
		double duration = drtServiceParams.duration;
		final Schedule schedule = vehicle.getSchedule();

		final Task currentTask = schedule.getCurrentTask();

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
				schedule.addTask(taskFactory.createDriveTask(vehicle, path, RELOCATE_SERVICE_TASK_TYPE)); // add RELOCATE
			}

			double startTime = path.getArrivalTime();
			double endTime = startTime + duration;

			addServiceTask(vehicle, startTime, endTime, toLink);

		} else if (currentTask instanceof EDrtChargingTask chargingTask &&
			Schedules.getLastTask(schedule) != currentTask &&
			!(Schedules.getNextTask(schedule) instanceof DrtServiceTask)) {

			// Append to charging task and keep position
			double compensatedDuration = compensateDuration(currentTask, duration, drtServiceParams.name);
			schedule.removeLastTask(); //Remove stay
			double startTime = currentTask.getEndTime();
			double endTime = startTime + compensatedDuration;
			addServiceTask(vehicle, startTime, endTime, chargingTask.getLink());
		} else {
			double compensatedDuration = compensateDuration(currentTask, duration, drtServiceParams.name);
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
					schedule.addTask(taskFactory.createDriveTask(vehicle, path, RELOCATE_SERVICE_TASK_TYPE)); // add RELOCATE
					double startTime = path.getArrivalTime();
					double endTime = startTime + compensatedDuration;

					addServiceTask(vehicle, startTime, endTime, toLink);
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
				double endTime = startTime + compensatedDuration;

				addServiceTask(vehicle, startTime, endTime, toLink);
			}
		}
	}

	private void addServiceTask(DvrpVehicle vehicle, double startTime, double endTime, Link link) {
		Schedule schedule = vehicle.getSchedule();
		// append DrtServiceTask
		schedule.addTask(taskFactory.createServiceTask(startTime, endTime, link));

		// append DrtStayTask
		schedule.addTask(taskFactory.createStayTask(vehicle, endTime, Math.max(vehicle.getServiceEndTime(), endTime), link));
	}

	double compensateDuration(Task currentTask, double requestedDuration, String serviceName) {
		if (JOINABLE_SERVICES.isStackableTask(currentTask)) {
			double currentTaskDuration = currentTask.getEndTime() - currentTask.getBeginTime();
			if (requestedDuration <= currentTaskDuration) {
				LOG.info("Service {} with requested {} seconds takes place while {}"
					,serviceName,requestedDuration, currentTask.getTaskType().name());
				return 1.;
			} else {
				double compensatedDuration = Math.max(1, requestedDuration - currentTaskDuration);
				LOG.info("Service {} with requested {} seconds takes partially place while {}. Remaining {} seconds"
					,serviceName,requestedDuration, currentTask.getTaskType().name(), compensatedDuration);
				return compensatedDuration;
			}
		}
		return requestedDuration;
	}


}
