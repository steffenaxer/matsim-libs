package org.matsim.contrib.drt.extension.operations.shifts.shift;

import java.util.*;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.operations.shifts.dispatcher.ShiftScheduler;
import org.matsim.contrib.drt.extension.operations.shifts.fleet.ShiftDvrpVehicle;
import org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics.ReassignLogic;
import org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics.UnassignLogic;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;

import com.google.common.collect.ImmutableMap;

import java.util.function.Function;

/**
 * @author steffenaxer
 */
public class OnDemandShiftScheduler implements ShiftScheduler {
	public static Function<DrtShiftSpecification, DrtShift> createShiftFromSpec = spec -> {
		DefaultShiftBreakImpl shiftBreak = null;
		DrtShiftBreakSpecification breakSpec = spec.getBreak().orElse(null);
		if (breakSpec != null) {
			shiftBreak = new DefaultShiftBreakImpl(
				breakSpec.getEarliestBreakStartTime(),
				breakSpec.getLatestBreakEndTime(),
				breakSpec.getDuration());
		}
		return (DrtShift) new DynamicDrtShiftImpl(spec.getId(), spec.getStartTime(), spec.getEndTime(),
			spec.getOperationFacilityId().orElse(null), spec.getDesignatedVehicleId().orElse(null),
			shiftBreak);
	};
	private final DrtShiftsSpecification shiftsSpecification;
	private final ReassignLogic reassignLogic;
	private final UnassignLogic unassignLogic;
	private Map<DvrpVehicle, Double> offlineVehicleMap;

	public OnDemandShiftScheduler(DrtShiftsSpecification shiftsSpecification,
								  ReassignLogic reassignLogic, UnassignLogic unassignLogic) {
		this.shiftsSpecification = shiftsSpecification;
		this.reassignLogic = reassignLogic;
		this.unassignLogic = unassignLogic;
	}

	boolean shouldUnassignLogic(ShiftDvrpVehicle vehicle, double simStep) {
		return this.unassignLogic.shouldUnassign(vehicle,simStep);
	}

	private List<DrtShift> shouldAssignLogic(double simStep, Fleet fleet) {
		List<DrtShiftSpecification> shiftSpecs = fleet.getVehicles().values().stream()
			.map(v -> this.reassignLogic.shouldReassign((ShiftDvrpVehicle) v, simStep))
			.filter(Optional::isPresent)
			.map(Optional::get).toList();

		// Add to shiftsSpecification for later analysis
		shiftSpecs.forEach(this.shiftsSpecification::addShiftSpecification);

		return shiftSpecs
			.stream()
			.map(createShiftFromSpec).toList();
	}

	private void unassignShifts(double simStep, Fleet fleet) {
		fleet.getVehicles().values().stream().filter(v -> this.shouldUnassignLogic((ShiftDvrpVehicle) v, simStep))
			.forEach(v -> {
				DynamicDrtShiftImpl dynamicDrtShift = (DynamicDrtShiftImpl) ((ShiftDvrpVehicle) v).getShifts().peek();
				if (dynamicDrtShift != null) {
					//Unassign now!
					dynamicDrtShift.setEndTime(simStep);
				}
			});
	}

	@Override
	public List<DrtShift> schedule(double time, Fleet fleet) {
		this.unassignShifts(time, fleet);
		return this.shouldAssignLogic(time, fleet);
	}

	@Override
	public ImmutableMap<Id<DrtShift>, DrtShift> initialSchedule() {
		return shiftsSpecification.getShiftSpecifications().values()
			.stream()
			.map(createShiftFromSpec)
			.collect(ImmutableMap.toImmutableMap(DrtShift::getId, s -> s));
	}

	@Override
	public DrtShiftsSpecification get() {
		return shiftsSpecification;
	}


}
