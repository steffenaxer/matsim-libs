package org.matsim.contrib.drt.extension.operations.shifts.shift;

import org.matsim.contrib.drt.extension.operations.shifts.dispatcher.ShiftScheduler;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeModule;

public class DynamicShiftModule extends AbstractDvrpModeModule {

	public DynamicShiftModule(DrtConfigGroup drtCfg) {
		super(drtCfg.getMode());

	}

	@Override
	public void install() {
		bindModal(ShiftScheduler.class).toProvider(modalProvider(getter -> new DynamicShiftScheduler(
			getter.getModal(Fleet.class)))).asEagerSingleton();
	}
}
