package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.extension.edrt.schedule.EDrtTaskFactoryImpl;
import org.matsim.contrib.drt.extension.edrt.scheduler.EmptyVehicleChargingScheduler;
import org.matsim.contrib.drt.extension.maintenance.tasks.EDrtMaintenanceTaskFactoryImpl;
import org.matsim.contrib.drt.prebooking.PrebookingActionCreator;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.drt.vrpagent.DrtActionCreator;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.core.mobsim.framework.MobsimTimer;

public class EDrtMaintenanceQSimModule extends AbstractDvrpModeQSimModule {
	DrtConfigGroup drtConfigGroup;

	public EDrtMaintenanceQSimModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.getMode());
		this.drtConfigGroup = drtConfigGroup;
	}

	@Override
	protected void configureQSim() {

		//set to null to avoid runtime exception
		bindModal(EmptyVehicleChargingScheduler.class).toProvider(modalProvider(
			getter -> null)
		).asEagerSingleton();

		bindModal(EDrtMaintenanceDynActionCreator.class).toProvider(modalProvider(getter -> {
			VrpAgentLogic.DynActionCreator delegate = drtConfigGroup.getPrebookingParams().isPresent()
				? getter.getModal(PrebookingActionCreator.class)
				: getter.getModal(DrtActionCreator.class);

			return new EDrtMaintenanceDynActionCreator(delegate, getter.get(MobsimTimer.class));
		})).asEagerSingleton();

		bindModal(VrpAgentLogic.DynActionCreator.class).to(modalKey(EDrtMaintenanceDynActionCreator.class));

		bindModal(DrtTaskFactory.class).toProvider(modalProvider(getter ->
		{
			DrtTaskFactory delegate = new EDrtTaskFactoryImpl();
			return new EDrtMaintenanceTaskFactoryImpl(delegate);
		})).asEagerSingleton();
	}
}
