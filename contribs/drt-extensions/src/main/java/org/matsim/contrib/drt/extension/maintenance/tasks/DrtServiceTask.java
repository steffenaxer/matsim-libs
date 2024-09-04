package org.matsim.contrib.drt.extension.maintenance.tasks;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.passenger.AcceptedDrtRequest;
import org.matsim.contrib.drt.schedule.DrtStayTask;
import org.matsim.contrib.drt.schedule.DrtStopTask;
import org.matsim.contrib.drt.schedule.DrtTaskType;
import org.matsim.contrib.dvrp.optimizer.Request;
import org.matsim.contrib.dvrp.schedule.DefaultStayTask;

import java.util.Collections;
import java.util.Map;

import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.STAY;
import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.STOP;

/**
 * @author steffenaxer
 */
public class DrtServiceTask extends DefaultStayTask  {

	public static final DrtTaskType TYPE = new DrtTaskType("SERVICE", STAY);

	public DrtServiceTask(double beginTime, double endTime, Link link) {
		super(TYPE,beginTime, endTime, link);
	}

}

