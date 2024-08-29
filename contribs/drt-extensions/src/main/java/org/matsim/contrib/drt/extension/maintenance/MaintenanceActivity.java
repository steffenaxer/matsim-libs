package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.dynagent.DynActivity;

public class MaintenanceActivity implements DynActivity {
	public static final String ACTIVITY_TYPE = "Maintenance";
	private final EDrtMaintenanceTask EDrtMaintenanceTask;

	public MaintenanceActivity(EDrtMaintenanceTask EDrtMaintenanceTask) {
		this.EDrtMaintenanceTask = EDrtMaintenanceTask;
	}

	@Override
	public String getActivityType() {
		return ACTIVITY_TYPE;
	}

	@Override
	public double getEndTime() {
		return EDrtMaintenanceTask.getEndTime();
	}

	@Override
	public void doSimStep(double now) {

	}


}
