package org.matsim.contrib.drt.extension.maintenance.services.conditions;

import org.matsim.contrib.drt.extension.maintenance.services.params.StopBasedConditionParam;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtServiceTask;
import org.matsim.contrib.drt.schedule.DrtStopTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;

/**
 * @author steffenaxer
 */
public class StopBasedServiceExecutionCondition implements ServiceExecutionCondition {
	private final StopBasedConditionParam stopBasedConditionParam;

	public StopBasedServiceExecutionCondition(StopBasedConditionParam stopBasedConditionParam)
	{
		this.stopBasedConditionParam = stopBasedConditionParam;
	}

	@Override
	public boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle);
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle)
	{
		return dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED &&
			calcStops(dvrpVehicle) > stopBasedConditionParam.requiredStops;
	}

	private int calcStops(DvrpVehicle dvrpVehicle) {
		double lastMaintenance = dvrpVehicle.getSchedule().getTasks().stream()
			.filter(t -> t instanceof DrtServiceTask)
			.mapToDouble(Task::getEndTime)
			.max()
			.orElse(0);

		return (int) dvrpVehicle.getSchedule().getTasks().stream()
			.filter(t -> t instanceof DrtStopTask)
			.filter(t -> t.getBeginTime() > lastMaintenance).count();
	}
}
