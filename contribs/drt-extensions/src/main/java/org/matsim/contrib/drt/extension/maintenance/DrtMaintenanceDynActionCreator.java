package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.extension.maintenance.tasks.DrtMaintenanceTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.schedule.Task;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.contrib.dynagent.DynAction;
import org.matsim.contrib.dynagent.DynAgent;
import org.matsim.core.mobsim.framework.MobsimTimer;

/**
 * @author steffenaxer
 */
public class DrtMaintenanceDynActionCreator implements VrpAgentLogic.DynActionCreator {
	private final VrpAgentLogic.DynActionCreator delegate;
	private final MobsimTimer timer;

	public DrtMaintenanceDynActionCreator(VrpAgentLogic.DynActionCreator delegate, MobsimTimer timer) {
		this.delegate = delegate;
		this.timer = timer;
	}

	public DynAction createAction(DynAgent dynAgent, DvrpVehicle vehicle, double now) {

		Task task = vehicle.getSchedule().getCurrentTask();
		if (task instanceof DrtMaintenanceTask eDrtMaintenanceTask) {
			return new MaintenanceActivity(eDrtMaintenanceTask);
		}

		return delegate.createAction(dynAgent, vehicle, now);
	}

}
