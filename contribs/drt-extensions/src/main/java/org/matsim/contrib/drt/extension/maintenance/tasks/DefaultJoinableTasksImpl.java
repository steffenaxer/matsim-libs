package org.matsim.contrib.drt.extension.maintenance.tasks;

import org.matsim.contrib.dvrp.schedule.Task;
import org.matsim.contrib.evrp.ChargingTask;

import java.util.Set;

/**
 * @author steffenaxer
 */
public class DefaultJoinableTasksImpl implements JoinableTasks {
	private final static Set<Class<? extends Task>> STACKABLE_TASKS = Set.of(DrtServiceTask.class, ChargingTask.class);

	@Override
	public boolean isStackableTask(Task task) {
		return STACKABLE_TASKS.stream().anyMatch(t -> t.isAssignableFrom(task.getClass()) );
	}
}
