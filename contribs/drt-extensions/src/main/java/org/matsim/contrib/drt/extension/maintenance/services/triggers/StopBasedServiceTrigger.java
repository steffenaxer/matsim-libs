package org.matsim.contrib.drt.extension.maintenance.services.triggers;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.maintenance.services.params.StopBasedTriggerParam;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtServiceTask;
import org.matsim.contrib.drt.schedule.DrtStopTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;

/**
 * @author steffenaxer
 */
public class StopBasedServiceTrigger extends AbstractTrigger {
	private final StopBasedTriggerParam stopBasedConditionParam;

	public StopBasedServiceTrigger(Id<DvrpVehicle> vehicleId, StopBasedTriggerParam stopBasedConditionParam)
	{
		super(vehicleId);
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
