package org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics;

import org.matsim.contrib.drt.extension.operations.shifts.fleet.ShiftDvrpVehicle;

/**
 * @author steffenaxer
 */
public interface UnassignLogic {
	boolean shouldUnassign(ShiftDvrpVehicle shiftDvrpVehicle, double time);
}
