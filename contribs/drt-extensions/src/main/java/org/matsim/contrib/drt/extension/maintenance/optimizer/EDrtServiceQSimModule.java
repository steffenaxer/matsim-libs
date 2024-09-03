package org.matsim.contrib.drt.extension.maintenance.optimizer;

import org.matsim.contrib.drt.extension.edrt.EDrtActionCreator;
import org.matsim.contrib.drt.extension.edrt.optimizer.EDrtVehicleDataEntryFactory;
import org.matsim.contrib.drt.extension.maintenance.schedule.EDrtServiceDynActionCreator;
import org.matsim.contrib.drt.extension.maintenance.tasks.EDrtServiceTaskFactoryImpl;
import org.matsim.contrib.drt.optimizer.VehicleEntry;
import org.matsim.contrib.drt.prebooking.PrebookingActionCreator;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.drt.vrpagent.DrtActionCreator;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.core.mobsim.framework.MobsimTimer;

public class EDrtServiceQSimModule extends AbstractDvrpModeQSimModule {
	DrtConfigGroup drtConfigGroup;

	public EDrtServiceQSimModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.getMode());
		this.drtConfigGroup = drtConfigGroup;
	}

	@Override
	protected void configureQSim() {

		bindModal(EDrtServiceDynActionCreator.class).toProvider(modalProvider(getter -> {
			VrpAgentLogic.DynActionCreator delegate = drtConfigGroup.getPrebookingParams().isPresent()
				? getter.getModal(PrebookingActionCreator.class)
				: getter.getModal(DrtActionCreator.class);

			return new EDrtServiceDynActionCreator(new EDrtActionCreator(delegate, getter.get(MobsimTimer.class)), getter.get(MobsimTimer.class));
		})).asEagerSingleton();

		bindModal(VrpAgentLogic.DynActionCreator.class).to(modalKey(EDrtServiceDynActionCreator.class));

		bindModal(DrtTaskFactory.class).toProvider(modalProvider(getter ->
			new EDrtServiceTaskFactoryImpl())).asEagerSingleton();

		bindModal(VehicleEntry.EntryFactory.class).toProvider(modalProvider(getter ->
			new ServiceEntryFactory(new EDrtVehicleDataEntryFactory(0)))).asEagerSingleton();
	}
}
