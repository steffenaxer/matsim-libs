package org.matsim.contrib.drt.extension.services.optimizer;

import org.matsim.contrib.drt.extension.services.tasks.DrtServiceTask;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;

/**
 * @author steffenaxer
 */
public class ServiceEntryFactory implements VehicleEntry.EntryFactory {
	private final VehicleEntry.EntryFactory delegate;

	public ServiceEntryFactory(VehicleEntry.EntryFactory delegate) {
		this.delegate = delegate;
	}

	@Override
	public VehicleEntry create(DvrpVehicle vehicle, double currentTime) {
		Schedule schedule = vehicle.getSchedule();
		int taskCount = schedule.getTaskCount();
		if (taskCount > 1) {
			Task oneBeforeLast = schedule.getTasks().get(taskCount - 2);
			if (oneBeforeLast.getStatus() != Task.TaskStatus.PERFORMED && oneBeforeLast.getTaskType()
				.equals(DrtServiceTask.TYPE)) {
				return null;
			}
		}

		return delegate.create(vehicle, currentTime);
	}
}
