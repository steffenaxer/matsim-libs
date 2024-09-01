package org.matsim.contrib.drt.extension.maintenance.logic;

import org.matsim.contrib.drt.extension.maintenance.tasks.DrtMaintenanceTask;
import org.matsim.contrib.drt.schedule.DrtStopTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;

/**
 * @author steffenaxer
 */
public class StopBasedMaintenanceRule implements MaintenanceRule {
	private final int requiredStops;

	public StopBasedMaintenanceRule(int requiredStops)
	{
		this.requiredStops = requiredStops;
	}

	@Override
	public boolean requiresMaintenance(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle);
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle)
	{
		return dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED &&
			calcStops(dvrpVehicle) > requiredStops;
	}

	private int calcStops(DvrpVehicle dvrpVehicle) {
		double lastMaintenance = dvrpVehicle.getSchedule().getTasks().stream()
			.filter(t -> t instanceof DrtMaintenanceTask)
			.mapToDouble(Task::getEndTime)
			.max()
			.orElse(0);

		return (int) dvrpVehicle.getSchedule().getTasks().stream()
			.filter(t -> t instanceof DrtStopTask)
			.filter(t -> t.getBeginTime() > lastMaintenance).count();
	}
}
