package org.matsim.contrib.drt.extension.services.services.triggers;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.services.services.params.TimeOfDayBasedTriggerParam;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;

/**
 * @author steffenaxer
 */
public class TimeOfDayBasedServiceTrigger implements ServiceExecutionTrigger {
	private final TimeOfDayBasedTriggerParam timeOfDayBasedConditionParam;

	public TimeOfDayBasedServiceTrigger(Id<DvrpVehicle> vehicleId, TimeOfDayBasedTriggerParam timeOfDayBasedConditionParam)
	{
		this.timeOfDayBasedConditionParam = timeOfDayBasedConditionParam;
	}

	@Override
	public boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle, timeStep);
	}

	@Override
	public String getName() {
		return timeOfDayBasedConditionParam.name;
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle, double timeStep)
	{
		return dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED
			&& timeStep==this.timeOfDayBasedConditionParam.executionTime;
	}

}
