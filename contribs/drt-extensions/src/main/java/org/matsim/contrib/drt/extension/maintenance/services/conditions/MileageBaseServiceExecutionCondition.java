package org.matsim.contrib.drt.extension.maintenance.services.conditions;

import com.google.common.collect.Streams;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.maintenance.services.params.MileageBasedConditionParam;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtServiceTask;
import org.matsim.contrib.drt.schedule.DrtDriveTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.path.VrpPath;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;

/**
 * @author steffenaxer
 */
public class MileageBaseServiceExecutionCondition implements ServiceExecutionCondition {
	private final MileageBasedConditionParam mileageBasedConditionParam;

	public MileageBaseServiceExecutionCondition(MileageBasedConditionParam mileageBasedConditionParam)
	{
		this.mileageBasedConditionParam = mileageBasedConditionParam;
	}

	@Override
	public boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep) {
		return this.judgeVehicle(dvrpVehicle, timeStep);
	}

	boolean judgeVehicle(DvrpVehicle dvrpVehicle, double timeStep)
	{
		if(dvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED)
		{
			double lastMaintenanceTime = dvrpVehicle.getSchedule().getTasks().stream()
				.filter(t -> t instanceof DrtServiceTask)
				.mapToDouble(Task::getEndTime)
				.max()
				.orElse(0);

			double drivenDistance = dvrpVehicle.getSchedule().getTasks().stream()
				.filter(t -> t instanceof DrtDriveTask)
				.filter(t -> t.getEndTime() > lastMaintenanceTime)
				.filter(t -> t.getEndTime() < timeStep)
				.mapToDouble(t -> getDistance(((DrtDriveTask) t).getPath()))
				.sum();

			return drivenDistance > mileageBasedConditionParam.requiredMileage;
		}

		return false;


	}

	double getDistance(VrpPath path)
	{
		return Streams.stream(path.iterator()).mapToDouble(Link::getLength).sum();
	}

}
