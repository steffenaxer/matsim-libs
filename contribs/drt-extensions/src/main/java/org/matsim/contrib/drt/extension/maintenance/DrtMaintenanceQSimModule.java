package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.extension.maintenance.dispatcher.MaintenanceTaskDispatcher;
import org.matsim.contrib.drt.extension.maintenance.dispatcher.MaintenanceTaskDispatcherImpl;
import org.matsim.contrib.drt.extension.maintenance.logic.*;
import org.matsim.contrib.drt.extension.maintenance.optimizer.MaintenanceTaskOptimizer;
import org.matsim.contrib.drt.extension.maintenance.scheduler.MaintenanceTaskScheduler;
import org.matsim.contrib.drt.extension.maintenance.scheduler.MaintenanceTaskSchedulerImpl;
import org.matsim.contrib.drt.extension.maintenance.tasks.DrtMaintenanceTaskFactoryImpl;
import org.matsim.contrib.drt.extension.maintenance.tasks.MaintenanceTaskFactory;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilityFinder;
import org.matsim.contrib.drt.optimizer.DefaultDrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.depot.DepotFinder;
import org.matsim.contrib.drt.optimizer.insertion.UnplannedRequestInserter;
import org.matsim.contrib.drt.optimizer.rebalancing.RebalancingStrategy;
import org.matsim.contrib.drt.prebooking.PrebookingActionCreator;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.drt.schedule.DrtTaskFactoryImpl;
import org.matsim.contrib.drt.scheduler.DrtScheduleInquiry;
import org.matsim.contrib.drt.scheduler.EmptyVehicleRelocator;
import org.matsim.contrib.drt.vrpagent.DrtActionCreator;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.schedule.ScheduleTimingUpdater;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.mobsim.framework.MobsimTimer;
import org.matsim.core.router.costcalculators.TravelDisutilityFactory;
import org.matsim.core.router.util.TravelTime;

public class DrtMaintenanceQSimModule extends AbstractDvrpModeQSimModule {
	DrtConfigGroup drtConfigGroup;

	public DrtMaintenanceQSimModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.getMode());
		this.drtConfigGroup = drtConfigGroup;
	}

	@Override
	protected void configureQSim() {
		bindModal(DrtMaintenanceDynActionCreator.class).toProvider(modalProvider(getter -> {
			VrpAgentLogic.DynActionCreator delegate = drtConfigGroup.getPrebookingParams().isPresent()
				? getter.getModal(PrebookingActionCreator.class)
				: getter.getModal(DrtActionCreator.class);

			return new DrtMaintenanceDynActionCreator(delegate, getter.get(MobsimTimer.class));
		})).asEagerSingleton();

		bindModal(VrpAgentLogic.DynActionCreator.class).to(modalKey(DrtMaintenanceDynActionCreator.class));


		bindModal(DrtTaskFactory.class).toProvider(modalProvider(getter ->
		{
			DrtTaskFactory delegate = new DrtTaskFactoryImpl();
			return new DrtMaintenanceTaskFactoryImpl(delegate);
		})).asEagerSingleton();
	}
}
