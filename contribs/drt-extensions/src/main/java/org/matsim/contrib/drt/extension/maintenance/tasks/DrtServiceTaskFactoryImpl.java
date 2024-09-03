package org.matsim.contrib.drt.extension.maintenance.tasks;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.schedule.*;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.path.VrpPathWithTravelData;
import org.matsim.contrib.dvrp.schedule.DefaultStayTask;

/**
 * @author steffenaxer
 */
public class DrtServiceTaskFactoryImpl implements ServiceTaskFactory {
	private final DrtTaskFactory delegate;

	public DrtServiceTaskFactoryImpl(DrtTaskFactory delegate) {
		this.delegate = delegate;
	}

	@Override
	public DrtServiceTask createServiceTask(double beginTime, double endTime, Link link) {
		return new DrtServiceTask(beginTime, endTime, link);
	}

	@Override
	public DrtDriveTask createDriveTask(DvrpVehicle vehicle, VrpPathWithTravelData path, DrtTaskType drtTaskType) {
		return delegate.createDriveTask(vehicle, path, drtTaskType);
	}

	@Override
	public DrtStopTask createStopTask(DvrpVehicle vehicle, double beginTime, double endTime, Link link) {
		return delegate.createStopTask(vehicle, beginTime, endTime, link);
	}

	@Override
	public DrtStayTask createStayTask(DvrpVehicle vehicle, double beginTime, double endTime, Link link) {
		return delegate.createStayTask(vehicle, beginTime, endTime, link);
	}

	@Override
	public DefaultStayTask createInitialTask(DvrpVehicle vehicle, double beginTime, double endTime, Link link) {
		return delegate.createInitialTask(vehicle, beginTime, endTime, link);
	}
}
