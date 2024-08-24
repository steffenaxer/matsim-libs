package org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics;

import org.matsim.contrib.drt.extension.operations.shifts.fleet.ShiftDvrpVehicle;
import org.matsim.contrib.drt.extension.operations.shifts.shift.DrtShiftSpecification;

import java.util.Optional;

/**
 * @author steffenaxer
 */
public interface ReassignLogic {
	Optional<DrtShiftSpecification> shouldReassign(ShiftDvrpVehicle shiftDvrpVehicle, double time);
}
