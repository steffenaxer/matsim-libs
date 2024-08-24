package org.matsim.contrib.drt.extension.operations.shifts.shift;

import org.matsim.contrib.drt.extension.operations.shifts.dispatcher.ShiftScheduler;
import org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics.FixedTimeReassignLogic;
import org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics.FixedTimeUnassignLogic;
import org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics.ReassignLogic;
import org.matsim.contrib.drt.extension.operations.shifts.shift.ondemandlogics.UnassignLogic;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeModule;
import org.matsim.core.controler.Controler;

/**
 * @author steffenaxer
 */
public class OnDemandShiftLogicModule extends AbstractDvrpModeModule {

	public OnDemandShiftLogicModule(DrtConfigGroup drtCfg) {
		super(drtCfg.getMode());
	}

	@Override
	public void install() {
		bindModal(ReassignLogic.class).toProvider(modalProvider(getter -> new FixedTimeReassignLogic(3600.))).asEagerSingleton();
		bindModal(UnassignLogic.class).toProvider(modalProvider(getter -> new FixedTimeUnassignLogic(3600.))).asEagerSingleton();

		bindModal(ShiftScheduler.class).toProvider(modalProvider(getter -> new OnDemandShiftScheduler(
			new DrtShiftsSpecificationImpl(), // Only use on demand shifts
			getter.getModal(ReassignLogic.class),
			getter.getModal(UnassignLogic.class)))).asEagerSingleton();
	}

}
