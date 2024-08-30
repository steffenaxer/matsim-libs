package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.extension.maintenance.tasks.DrtMaintenanceTask;
import org.matsim.contrib.dynagent.DynActivity;

public class MaintenanceActivity implements DynActivity {
	public static final String ACTIVITY_TYPE = "Maintenance";
	private final DrtMaintenanceTask drtMaintenanceTask;

	public MaintenanceActivity(DrtMaintenanceTask drtMaintenanceTask) {
		this.drtMaintenanceTask = drtMaintenanceTask;
	}

	@Override
	public String getActivityType() {
		return ACTIVITY_TYPE;
	}

	@Override
	public double getEndTime() {
		return this.drtMaintenanceTask.getEndTime();
	}

	@Override
	public void doSimStep(double now) {

	}


}
