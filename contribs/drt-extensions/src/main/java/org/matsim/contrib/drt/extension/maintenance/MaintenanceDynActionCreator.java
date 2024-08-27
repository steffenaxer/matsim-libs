package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.extension.operations.eshifts.charging.ChargingBreakActivity;
import org.matsim.contrib.drt.extension.operations.eshifts.charging.ChargingChangeoverActivity;
import org.matsim.contrib.drt.extension.operations.eshifts.charging.ChargingWaitForShiftActivity;
import org.matsim.contrib.drt.extension.operations.eshifts.schedule.EDrtShiftBreakTaskImpl;
import org.matsim.contrib.drt.extension.operations.eshifts.schedule.EDrtShiftChangeoverTaskImpl;
import org.matsim.contrib.drt.extension.operations.eshifts.schedule.EDrtWaitForShiftTask;
import org.matsim.contrib.drt.passenger.DrtStopActivity;
import org.matsim.contrib.drt.schedule.DrtStopTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.passenger.PassengerHandler;
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
public class MaintenanceDynActionCreator implements VrpAgentLogic.DynActionCreator {
	private final VrpAgentLogic.DynActionCreator delegate;
	private final MobsimTimer timer;

	public MaintenanceDynActionCreator(VrpAgentLogic.DynActionCreator delegate, MobsimTimer timer) {
		this.delegate = delegate;
		this.timer = timer;
	}

	public DynAction createAction(DynAgent dynAgent, DvrpVehicle vehicle, double now) {

		Task task = vehicle.getSchedule().getCurrentTask();
		if (task instanceof MaintenanceTask maintenanceTask) {
			task.initTaskTracker(new OfflineETaskTracker((EvDvrpVehicle) vehicle, timer));
			return new MaintenanceActivity(maintenanceTask);
		}

		DynAction dynAction = delegate.createAction(dynAgent, vehicle, now);
		if (task.getTaskTracker() == null) {
			task.initTaskTracker(new OfflineETaskTracker((EvDvrpVehicle) vehicle, timer));
		}
		return dynAction;
	}

}
