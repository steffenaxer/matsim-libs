package org.matsim.contrib.drt.extension.operations.shifts.shift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.operations.shifts.dispatcher.ShiftScheduler;
import org.matsim.contrib.drt.extension.operations.shifts.fleet.ShiftDvrpVehicle;
import org.matsim.contrib.drt.extension.operations.shifts.schedule.WaitForShiftTask;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.schedule.Schedule;

import com.google.common.collect.ImmutableMap;

import java.util.function.Function;


public class DynamicShiftScheduler implements ShiftScheduler {
	private static final double SHIFT_LEGNTH = 2 * 3600.;
	private final Fleet fleet;
	private final Map<DvrpVehicle, Double> offlineVehicleMap = new HashMap<>();
	private final DrtShiftsSpecification shiftsSpecification;
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

	DynamicShiftScheduler(Fleet fleet) {
		this.fleet = fleet;
		this.shiftsSpecification = createShiftsFromFleet(fleet);

	}

	DrtShiftsSpecification createShiftsFromFleet(Fleet fleet) {

		DrtShiftsSpecification drtShiftsSpecification = new DrtShiftsSpecificationImpl();
		for (var vehicle : fleet.getVehicles().values()) {
			DrtShiftSpecificationImpl.Builder builder = DrtShiftSpecificationImpl.newBuilder();
			builder.id(Id.create(UUID.randomUUID().toString(), DrtShift.class));
			builder.start(vehicle.getServiceBeginTime() + 10.);
			builder.end(vehicle.getServiceEndTime() - 10);
			drtShiftsSpecification.addShiftSpecification(builder.build());
		}

		return drtShiftsSpecification;
	}

	boolean shouldUnassignShiftFromVehicle(ShiftDvrpVehicle vehicle, double simStep) {

		DrtShift currentShift = vehicle.getShifts().peek();
		if (vehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED &&
			currentShift != null &&
			currentShift.isStarted() &&
			!currentShift.isEnded()) {
			double shiftDuration = simStep - currentShift.getStartTime();
			return shiftDuration % SHIFT_LEGNTH == 0 && shiftDuration > 0;

		}

		return false;
	}

	private List<DrtShift> reassignShifts(double simStep) {
		List<DrtShift> assignableShifts = new ArrayList<>();
		for (DvrpVehicle v : this.offlineVehicleMap.keySet()) {
			ShiftDvrpVehicle vehicle = (ShiftDvrpVehicle) v;
			double offlineTime = this.offlineVehicleMap.get(vehicle);
			if (vehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED
				&& vehicle.getSchedule().getCurrentTask() instanceof WaitForShiftTask
				&& (simStep - offlineTime) > SHIFT_LEGNTH) {
				DynamicDrtShiftImpl shift = new DynamicDrtShiftImpl(Id.create(UUID.randomUUID().toString(), DrtShift.class),
									simStep + 1,
									vehicle.getSpecification().getServiceEndTime() - 10., null, v.getId(), null);
				assignableShifts.add(shift);
			}
		}

		return assignableShifts;
	}

	private void unassignShifts(double simStep) {
		fleet.getVehicles().values().stream().filter(v -> shouldUnassignShiftFromVehicle((ShiftDvrpVehicle) v, simStep))
			.forEach(v -> {
				var tasks = v.getSchedule().getTasks();
				double endTime = tasks.get(tasks.size() - 2).getEndTime();
				DynamicDrtShiftImpl dynamicDrtShift = (DynamicDrtShiftImpl) ((ShiftDvrpVehicle) v).getShifts().peek();
				dynamicDrtShift.setEndTime(endTime);
			});
	}

	void checkVehiclesWithoutShift(double timeStep, Fleet fleet) {
		for (DvrpVehicle v : fleet.getVehicles().values()) {
			ShiftDvrpVehicle vehicle = (ShiftDvrpVehicle) v;
			if (vehicle.getSchedule().getStatus() == Schedule.ScheduleStatus.STARTED &&
				vehicle.getSchedule().getCurrentTask() instanceof WaitForShiftTask) {
				this.offlineVehicleMap.computeIfAbsent(v, k -> timeStep);
			} else {
				offlineVehicleMap.remove(vehicle);
			}
		}
	}


	@Override
	public List<DrtShift> schedule(double time, Fleet fleet) {
		checkVehiclesWithoutShift(time, fleet);
		unassignShifts(time);
		return this.reassignShifts(time);
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
