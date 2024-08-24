package org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics;

import org.matsim.contrib.drt.extension.operations.shifts.fleet.ShiftDvrpVehicle;
import org.matsim.contrib.drt.extension.operations.shifts.shift.DrtShift;
import org.matsim.contrib.dvrp.schedule.Schedule;

/**
 * @author steffenaxer
 */
public class FixedTimeUnassignLogic implements UnassignLogic {
	double maxAssignedTime;

	public FixedTimeUnassignLogic(double maxAssignedTime) {
		this.maxAssignedTime = maxAssignedTime;
	}

	@Override
	public boolean shouldUnassign(ShiftDvrpVehicle shiftDvrpVehicle, double time) {
		DrtShift currentShift = shiftDvrpVehicle.getShifts().peek();
		if (shiftDvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED &&
			currentShift != null &&
			currentShift.isStarted() &&
			!currentShift.isEnded()) {
			double shiftDuration = time - currentShift.getStartTime();
			return shiftDuration > maxAssignedTime;
		}

		return false;
	}
}
