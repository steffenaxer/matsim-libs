package org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.operations.shifts.fleet.ShiftDvrpVehicle;
import org.matsim.contrib.drt.extension.operations.shifts.schedule.WaitForShiftTask;
import org.matsim.contrib.drt.extension.operations.shifts.shift.DrtShift;
import org.matsim.contrib.drt.extension.operations.shifts.shift.DrtShiftSpecification;
import org.matsim.contrib.drt.extension.operations.shifts.shift.DrtShiftSpecificationImpl;
import org.matsim.contrib.dvrp.schedule.Schedule;

import java.util.Optional;
import java.util.UUID;

/**
 * @author steffenaxer
 */
public class FixedTimeReassignLogic implements ReassignLogic {
	double maxUnassignedTime;

	public FixedTimeReassignLogic(double maxUnassignedTime) {
		this.maxUnassignedTime = maxUnassignedTime;
	}


	@Override
	public Optional<DrtShiftSpecification> shouldReassign(ShiftDvrpVehicle shiftDvrpVehicle, double time) {

		if (shiftDvrpVehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED
			&& shiftDvrpVehicle.getSchedule().getCurrentTask() instanceof WaitForShiftTask
			&& (time - shiftDvrpVehicle.getSchedule().getCurrentTask().getBeginTime()) > maxUnassignedTime) {
			DrtShiftSpecificationImpl.Builder builder =  DrtShiftSpecificationImpl.newBuilder() ;
			builder.id(Id.create(UUID.randomUUID().toString(), DrtShift.class));
			builder.start(time + 1);
			builder.end(shiftDvrpVehicle.getSpecification().getServiceEndTime());
			builder.designatedVehicle(shiftDvrpVehicle.getId());
			return Optional.of(builder.build());
		}

		return Optional.empty();
	}
}
