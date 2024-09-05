package org.matsim.contrib.drt.extension.maintenance.tasks;

import org.matsim.contrib.dvrp.schedule.Task;

/**
 * @author steffenaxer
 */
public interface JoinableTasks {
	boolean isStackableTask(Task task);
}
