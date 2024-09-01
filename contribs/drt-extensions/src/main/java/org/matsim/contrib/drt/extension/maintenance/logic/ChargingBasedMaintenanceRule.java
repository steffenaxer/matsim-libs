package org.matsim.contrib.drt.extension.maintenance.logic;

import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.evrp.ChargingTask;

/**
 * @author steffenaxer
 */
public class ChargingBasedMaintenanceRule implements MaintenanceRule {

	@Override
	public boolean requiresMaintenance(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle);
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle)
	{
		return dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED &&
			dvrpVehicle.getSchedule().getCurrentTask() instanceof ChargingTask;
	}

}
