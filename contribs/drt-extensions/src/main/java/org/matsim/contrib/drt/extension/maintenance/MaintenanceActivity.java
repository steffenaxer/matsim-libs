package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.dynagent.DynActivity;

public class MaintenanceActivity implements DynActivity {
	public static final String ACTIVITY_TYPE = "Maintenance";
	private final MaintenanceTask maintenanceTask;

	public MaintenanceActivity(MaintenanceTask maintenanceTask) {
		this.maintenanceTask = maintenanceTask;
	}

	@Override
	public String getActivityType() {
		return ACTIVITY_TYPE;
	}

	@Override
	public double getEndTime() {
		return maintenanceTask.getEndTime();
	}

	@Override
	public void doSimStep(double now) {

	}


}
