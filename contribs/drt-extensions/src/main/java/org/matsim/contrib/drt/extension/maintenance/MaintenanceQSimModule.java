package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.prebooking.PrebookingActionCreator;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.vrpagent.DrtActionCreator;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.core.mobsim.framework.MobsimTimer;

public class MaintenanceQSimModule extends AbstractDvrpModeQSimModule {
	DrtConfigGroup drtConfigGroup;

	public MaintenanceQSimModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.getMode());
		this.drtConfigGroup = drtConfigGroup;
	}

	@Override
	protected void configureQSim() {
		bindModal(MaintenanceDynActionCreator.class).toProvider(modalProvider(getter -> {
			VrpAgentLogic.DynActionCreator delegate = drtConfigGroup.getPrebookingParams().isPresent()
				? getter.getModal(PrebookingActionCreator.class)
				: getter.getModal(DrtActionCreator.class);
			return new MaintenanceDynActionCreator(delegate, getter.get(MobsimTimer.class));
		})).asEagerSingleton();

		bindModal(VrpAgentLogic.DynActionCreator.class).to(modalKey(MaintenanceDynActionCreator.class));
	}
}