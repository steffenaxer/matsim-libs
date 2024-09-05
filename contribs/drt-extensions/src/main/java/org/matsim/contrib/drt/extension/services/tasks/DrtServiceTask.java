package org.matsim.contrib.drt.extension.services.tasks;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.schedule.DrtTaskType;
import org.matsim.contrib.dvrp.schedule.DefaultStayTask;

import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.STAY;

/**
 * @author steffenaxer
 */
public class DrtServiceTask extends DefaultStayTask  {

	public static final DrtTaskType TYPE = new DrtTaskType("SERVICE", STAY);

	public DrtServiceTask(double beginTime, double endTime, Link link) {
		super(TYPE,beginTime, endTime, link);
	}

}

