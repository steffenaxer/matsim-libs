package org.matsim.contrib.drt.extension.maintenance.tasks;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;

/**
 * @author steffenaxer
 */
public interface MaintenanceTaskFactory extends DrtTaskFactory {
	DrtMaintenanceTask createMaintenanceTask(double beginTime, double endTime, Link link);
}
