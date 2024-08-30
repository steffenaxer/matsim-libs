package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.extension.maintenance.tasks.EDrtMaintenanceTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Task;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.contrib.dynagent.DynAction;
import org.matsim.contrib.dynagent.DynAgent;
import org.matsim.contrib.evrp.EvDvrpVehicle;
import org.matsim.contrib.evrp.tracker.OfflineETaskTracker;
import org.matsim.core.mobsim.framework.MobsimTimer;

/**
 * @author steffenaxer
 */
public class EDrtMaintenanceDynActionCreator implements VrpAgentLogic.DynActionCreator {
	private final VrpAgentLogic.DynActionCreator delegate;
	private final MobsimTimer timer;

	public EDrtMaintenanceDynActionCreator(VrpAgentLogic.DynActionCreator delegate, MobsimTimer timer) {
		this.delegate = delegate;
		this.timer = timer;
	}

	public DynAction createAction(DynAgent dynAgent, DvrpVehicle vehicle, double now) {

		Task task = vehicle.getSchedule().getCurrentTask();
		if (task instanceof EDrtMaintenanceTask EDrtMaintenanceTask) {
			task.initTaskTracker(new OfflineETaskTracker((EvDvrpVehicle) vehicle, timer));
			return new MaintenanceActivity(EDrtMaintenanceTask);
		}

		DynAction dynAction = delegate.createAction(dynAgent, vehicle, now);
		if (task.getTaskTracker() == null) {
			task.initTaskTracker(new OfflineETaskTracker((EvDvrpVehicle) vehicle, timer));
		}
		return dynAction;
	}

}
