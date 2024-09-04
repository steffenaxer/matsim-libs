package org.matsim.contrib.drt.extension.maintenance.services.conditions;

import org.matsim.contrib.drt.extension.maintenance.services.params.TimeOfDayBasedConditionParam;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;

/**
 * @author steffenaxer
 */
public class TimeOfDayBaseServiceExecutionCondition implements ServiceExecutionCondition, TimedCondition {
	private final TimeOfDayBasedConditionParam timeOfDayBasedConditionParam;

	public TimeOfDayBaseServiceExecutionCondition(TimeOfDayBasedConditionParam timeOfDayBasedConditionParam)
	{
		this.timeOfDayBasedConditionParam = timeOfDayBasedConditionParam;
	}

	@Override
	public boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle, timeStep);
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle, double timeStep)
	{
		return dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED
			&& timeStep==this.timeOfDayBasedConditionParam.serviceTimeOfDay;
	}

	@Override
	public double getExecutionTime() {
		return this.timeOfDayBasedConditionParam.serviceTimeOfDay;
	}
}
