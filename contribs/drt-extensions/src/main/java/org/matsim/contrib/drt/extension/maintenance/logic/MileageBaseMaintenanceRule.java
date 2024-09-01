package org.matsim.contrib.drt.extension.maintenance.logic;

import com.google.common.collect.Streams;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtMaintenanceTask;
import org.matsim.contrib.drt.schedule.DrtDriveTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.path.VrpPath;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;

/**
 * @author steffenaxer
 */
public class MileageBaseMaintenanceRule implements MaintenanceRule {
	private final double requiredMileage;

	public MileageBaseMaintenanceRule(double requiredMileage)
	{
		this.requiredMileage = requiredMileage;
	}

	@Override
	public boolean requiresMaintenance(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle, timeStep);
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle, double timeStep)
	{
		if(dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED)
		{
			double lastMaintenanceTime = dvrpVehicle.getSchedule().getTasks().stream()
				.filter(t -> t instanceof DrtMaintenanceTask)
				.mapToDouble(Task::getEndTime)
				.max()
				.orElse(0);

			double drivenDistance = dvrpVehicle.getSchedule().getTasks().stream()
				.filter(t -> t instanceof DrtDriveTask)
				.filter(t -> t.getEndTime() > lastMaintenanceTime)
				.filter(t -> t.getEndTime() < timeStep)
				.mapToDouble(t -> getDistance(((DrtDriveTask) t).getPath()))
				.sum();

			return drivenDistance > this.requiredMileage;
		}

		return false;


	}

	double getDistance(VrpPath path)
	{
		return Streams.stream(path.iterator()).mapToDouble(Link::getLength).sum();
	}

}
