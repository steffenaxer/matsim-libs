package org.matsim.contrib.drt.extension.services.tasks;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;

/**
 * @author steffenaxer
 */
public interface ServiceTaskFactory extends DrtTaskFactory {
	DrtServiceTask createServiceTask(double beginTime, double endTime, Link link);
}
