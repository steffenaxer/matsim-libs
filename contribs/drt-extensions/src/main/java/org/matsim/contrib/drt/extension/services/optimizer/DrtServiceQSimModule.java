package org.matsim.contrib.drt.extension.services.optimizer;

import org.matsim.contrib.drt.extension.services.schedule.DrtServiceDynActionCreator;
import org.matsim.contrib.drt.extension.services.tasks.DrtServiceTaskFactoryImpl;
import org.matsim.contrib.drt.prebooking.PrebookingActionCreator;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.drt.schedule.DrtTaskFactoryImpl;
import org.matsim.contrib.drt.vrpagent.DrtActionCreator;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.core.mobsim.framework.MobsimTimer;

public class DrtServiceQSimModule extends AbstractDvrpModeQSimModule {
	DrtConfigGroup drtConfigGroup;

	public DrtServiceQSimModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.getMode());
		this.drtConfigGroup = drtConfigGroup;
	}

	@Override
	protected void configureQSim() {
		bindModal(DrtServiceDynActionCreator.class).toProvider(modalProvider(getter -> {
			VrpAgentLogic.DynActionCreator delegate = drtConfigGroup.getPrebookingParams().isPresent()
				? getter.getModal(PrebookingActionCreator.class)
				: getter.getModal(DrtActionCreator.class);

			return new DrtServiceDynActionCreator(delegate, getter.get(MobsimTimer.class));
		})).asEagerSingleton();

		bindModal(VrpAgentLogic.DynActionCreator.class).to(modalKey(DrtServiceDynActionCreator.class));


		bindModal(DrtTaskFactory.class).toProvider(modalProvider(getter ->
		{
			DrtTaskFactory delegate = new DrtTaskFactoryImpl();
			return new DrtServiceTaskFactoryImpl(delegate);
		})).asEagerSingleton();
	}
}
