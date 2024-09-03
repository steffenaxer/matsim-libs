package org.matsim.contrib.drt.extension.maintenance.schedule;

import org.matsim.contrib.drt.extension.maintenance.tasks.DrtServiceTask;
import org.matsim.contrib.dynagent.DynActivity;

public class ServiceActivity implements DynActivity {
	public static final String ACTIVITY_TYPE = "Service";
	private final DrtServiceTask drtServiceTask;

	public ServiceActivity(DrtServiceTask drtServiceTask) {
		this.drtServiceTask = drtServiceTask;
	}

	@Override
	public String getActivityType() {
		return ACTIVITY_TYPE;
	}

	@Override
	public double getEndTime() {
		return this.drtServiceTask.getEndTime();
	}

	@Override
	public void doSimStep(double now) {

	}


}
