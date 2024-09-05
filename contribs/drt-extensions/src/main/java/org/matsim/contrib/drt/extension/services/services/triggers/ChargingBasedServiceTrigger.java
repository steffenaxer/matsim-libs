package org.matsim.contrib.drt.extension.services.services.triggers;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.services.services.params.ChargingBasedTriggerParam;
import org.matsim.contrib.drt.extension.services.tasks.DrtServiceTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Schedules;
import org.matsim.contrib.evrp.ChargingTask;

/**
 * @author steffenaxer
 */
public class ChargingBasedServiceTrigger implements ServiceExecutionTrigger {
	ChargingBasedTriggerParam chargingBasedConditionParam;

	public ChargingBasedServiceTrigger(Id<DvrpVehicle> vehicleId, ChargingBasedTriggerParam chargingBasedConditionParam) {

		this.chargingBasedConditionParam = chargingBasedConditionParam;
	}

	@Override
	public boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle);
	}

	@Override
	public String getName() {
		return chargingBasedConditionParam.name;
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle)
	{
		Schedule schedule = dvrpVehicle.getSchedule();
		return schedule.getStatus() == Schedule.ScheduleStatus.STARTED &&
			schedule.getCurrentTask() instanceof ChargingTask &&
			!Schedules.getNextTask(schedule).getTaskType().equals(DrtServiceTask.TYPE);
	}

}
