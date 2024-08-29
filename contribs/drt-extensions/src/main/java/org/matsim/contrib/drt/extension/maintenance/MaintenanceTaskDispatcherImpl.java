package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilities;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilityType;
import org.matsim.contrib.drt.schedule.DrtStayTask;
import org.matsim.contrib.drt.schedule.DrtStopTask;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Schedules;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.facilities.Facility;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author steffenaxer
 */
public class MaintenanceTaskDispatcherImpl implements MaintenanceTaskDispatcher {
	private final double MAINTENANCE_TIME = 900.;
	private final int STOP_LIMIT = 30;
	private final Fleet fleet;
	private final Random random = MatsimRandom.getLocalInstance();
	private final EventsManager eventsManager;
	private final DrtTaskFactory taskFactory;
	private final Map<Id<DvrpVehicle>, Double> lastCleaningMap = new HashMap<>();
	private final Map<Id<Link>, OperationFacility> hubLocations;

	public MaintenanceTaskDispatcherImpl(Fleet fleet, EventsManager eventsManager, DrtTaskFactory taskFactory, OperationFacilities operationFacilities) {
		this.fleet = fleet;
		this.eventsManager = eventsManager;
		this.taskFactory = taskFactory;
		this.hubLocations = operationFacilities.getDrtOperationFacilities().values().stream()
			.filter(o -> o.getType().equals(OperationFacilityType.hub))
			.collect(Collectors.toMap(Facility::getLinkId, o -> o));
	}

	@Override
	public void dispatch(double timeStep) {
		for (DvrpVehicle dvrpVehicle : fleet.getVehicles().values()) {
			scheduleMaintenanceTask(dvrpVehicle, timeStep);
		}
	}

	private int countStopsTillLastCleaning(DvrpVehicle dvrpVehicle) {
		return (int) dvrpVehicle.getSchedule().getTasks().stream()
			.filter(t -> t instanceof DrtStopTask)
			.filter(t -> t.getBeginTime() > this.lastCleaningMap.getOrDefault(dvrpVehicle.getId(), 0.)).count();
	}

	private void scheduleMaintenanceTask(DvrpVehicle dvrpVehicle, double timeStep) {
		if (dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED &&
			dvrpVehicle.getSchedule().getCurrentTask() instanceof DrtStayTask drtStayTask &&
			Schedules.getLastTask(dvrpVehicle.getSchedule()).equals(drtStayTask) && // current task is last task
			hubLocations.containsKey(drtStayTask.getLink().getId()) && // is at hub
			countStopsTillLastCleaning(dvrpVehicle) > STOP_LIMIT) { // needs cleaning

			Schedule schedule = dvrpVehicle.getSchedule();

			// End current task
			drtStayTask.setEndTime(timeStep);

			// Add a new maintenance task
			schedule.addTask(new EDrtMaintenanceTask(timeStep, timeStep + MAINTENANCE_TIME, drtStayTask.getLink(), hubLocations.get(drtStayTask.getLink().getId())));
			lastCleaningMap.put(dvrpVehicle.getId(), timeStep);

			// Append stay
			schedule.addTask(taskFactory.createStayTask(dvrpVehicle, timeStep + MAINTENANCE_TIME, dvrpVehicle.getServiceEndTime(), drtStayTask.getLink()));
		}
	}
}
