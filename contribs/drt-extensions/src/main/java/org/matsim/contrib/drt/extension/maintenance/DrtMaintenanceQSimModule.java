package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.extension.maintenance.dispatcher.MaintenanceTaskDispatcher;
import org.matsim.contrib.drt.extension.maintenance.dispatcher.MaintenanceTaskDispatcherImpl;
import org.matsim.contrib.drt.extension.maintenance.logic.MaintenanceLogic;
import org.matsim.contrib.drt.extension.maintenance.logic.StopBasedMaintenanceLogic;
import org.matsim.contrib.drt.extension.maintenance.optimizer.MaintenanceTaskOptimizer;
import org.matsim.contrib.drt.extension.maintenance.scheduler.MaintenanceTaskScheduler;
import org.matsim.contrib.drt.extension.maintenance.scheduler.MaintenanceTaskSchedulerImpl;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilities;
import org.matsim.contrib.drt.optimizer.DefaultDrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.depot.DepotFinder;
import org.matsim.contrib.drt.optimizer.insertion.UnplannedRequestInserter;
import org.matsim.contrib.drt.optimizer.rebalancing.RebalancingStrategy;
import org.matsim.contrib.drt.prebooking.PrebookingActionCreator;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
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

		bindModal(MaintenanceLogic.class).toProvider(modalProvider(getter -> new StopBasedMaintenanceLogic(30)))
			.asEagerSingleton();

		bindModal(MaintenanceTaskDispatcher.class).toProvider(modalProvider(getter -> new MaintenanceTaskDispatcherImpl(
			getter.getModal(Fleet.class),
			getter.get(EventsManager.class),
			getter.getModal(OperationFacilities.class),
			getter.getModal(MaintenanceTaskScheduler.class),
			getter.getModal(MaintenanceLogic.class)))).asEagerSingleton();

		bindModal(MaintenanceTaskScheduler.class).toProvider(modalProvider(getter -> new MaintenanceTaskSchedulerImpl(
			getter.getModal(Network.class),
			getter.getModal(TravelTime.class),
			getter.getModal(TravelDisutilityFactory.class).createTravelDisutility(getter.getModal(TravelTime.class)),
			getter.get(MobsimTimer.class),
			getter.getModal(DrtTaskFactory.class)))).asEagerSingleton();

		addModalComponent(DrtOptimizer.class, modalProvider(
			getter -> {
				DrtOptimizer delegate = new DefaultDrtOptimizer(drtConfigGroup, getter.getModal(Fleet.class), getter.get(MobsimTimer.class),
					getter.getModal(DepotFinder.class), getter.getModal(RebalancingStrategy.class),
					getter.getModal(DrtScheduleInquiry.class), getter.getModal(ScheduleTimingUpdater.class),
					getter.getModal(EmptyVehicleRelocator.class), getter.getModal(UnplannedRequestInserter.class),
					getter.getModal(DrtRequestInsertionRetryQueue.class)
				);

				return new MaintenanceTaskOptimizer(getter.getModal(MaintenanceTaskDispatcher.class),
					delegate,
					getter.getModal(ScheduleTimingUpdater.class));
			}));

	}
}
