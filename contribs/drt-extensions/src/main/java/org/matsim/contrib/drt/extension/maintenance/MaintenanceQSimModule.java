package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.contrib.drt.extension.operations.eshifts.schedule.ShiftEDrtActionCreator;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilities;
import org.matsim.contrib.drt.extension.operations.shifts.dispatcher.DrtShiftDispatcher;
import org.matsim.contrib.drt.extension.operations.shifts.optimizer.ShiftDrtOptimizer;
import org.matsim.contrib.drt.extension.operations.shifts.schedule.ShiftDrtActionCreator;
import org.matsim.contrib.drt.extension.operations.shifts.schedule.ShiftDrtTaskFactory;
import org.matsim.contrib.drt.optimizer.DefaultDrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.depot.DepotFinder;
import org.matsim.contrib.drt.optimizer.insertion.UnplannedRequestInserter;
import org.matsim.contrib.drt.optimizer.rebalancing.RebalancingStrategy;
import org.matsim.contrib.drt.prebooking.PrebookingActionCreator;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.scheduler.DrtScheduleInquiry;
import org.matsim.contrib.drt.scheduler.EmptyVehicleRelocator;
import org.matsim.contrib.drt.vrpagent.DrtActionCreator;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.passenger.PassengerHandler;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.schedule.ScheduleTimingUpdater;
import org.matsim.contrib.dvrp.vrpagent.VrpAgentLogic;
import org.matsim.core.api.experimental.events.EventsManager;
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

			return new MaintenanceDynActionCreator(new ShiftEDrtActionCreator(
				new ShiftDrtActionCreator(getter.getModal(PassengerHandler.class), delegate),
				getter.get(MobsimTimer.class), getter.getModal(PassengerHandler.class)), getter.get(MobsimTimer.class));
		})).asEagerSingleton();

		bindModal(VrpAgentLogic.DynActionCreator.class).to(modalKey(MaintenanceDynActionCreator.class));
		bindModal(MaintenanceTaskDispatcher.class).toProvider(modalProvider(getter -> new MaintenanceTaskDispatcherImpl(
			getter.getModal(Fleet.class),
			getter.get(EventsManager.class),
			getter.getModal(ShiftDrtTaskFactory.class),
			getter.getModal(OperationFacilities.class)))).asEagerSingleton();


		addModalComponent(DrtOptimizer.class, modalProvider(
			getter -> {
				DrtOptimizer delegate = new ShiftDrtOptimizer(
					new DefaultDrtOptimizer(drtConfigGroup, getter.getModal(Fleet.class), getter.get(MobsimTimer.class),
						getter.getModal(DepotFinder.class), getter.getModal(RebalancingStrategy.class),
						getter.getModal(DrtScheduleInquiry.class), getter.getModal(ScheduleTimingUpdater.class),
						getter.getModal(EmptyVehicleRelocator.class), getter.getModal(UnplannedRequestInserter.class),
						getter.getModal(DrtRequestInsertionRetryQueue.class)
					),
					getter.getModal(DrtShiftDispatcher.class),
					getter.getModal(ScheduleTimingUpdater.class));

				return new MaintenanceTaskOptimizer(getter.getModal(MaintenanceTaskDispatcher.class),
					delegate,
					getter.getModal(ScheduleTimingUpdater.class));
			}));

	}
}
