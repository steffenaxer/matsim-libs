package org.matsim.contrib.drt.extension.maintenance.services.conditions;

import org.matsim.contrib.drt.extension.maintenance.services.params.ChargingBasedConditionParam;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtServiceTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Schedules;
import org.matsim.contrib.evrp.ChargingTask;

/**
 * @author steffenaxer
 */
public class ChargingBasedServiceExecutionCondition implements ServiceExecutionCondition {

	public ChargingBasedServiceExecutionCondition(ChargingBasedConditionParam chargingBasedConditionParam) {
	}

	@Override
	public boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle);
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle)
	{
		Schedule schedule = dvrpVehicle.getSchedule();
		return schedule.getStatus() == Schedule.ScheduleStatus.STARTED &&
			schedule.getCurrentTask() instanceof ChargingTask &&
			!Schedules.getNextTask(schedule).getTaskType().equals(DrtServiceTask.TYPE);
	}

}
