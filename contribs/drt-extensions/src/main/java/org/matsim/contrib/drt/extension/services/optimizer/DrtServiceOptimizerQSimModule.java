package org.matsim.contrib.drt.extension.services.optimizer;

import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.services.dispatcher.ServiceTaskDispatcher;
import org.matsim.contrib.drt.extension.services.dispatcher.ServiceTaskDispatcherImpl;
import org.matsim.contrib.drt.extension.services.services.*;
import org.matsim.contrib.drt.extension.services.schedule.ServiceTaskScheduler;
import org.matsim.contrib.drt.extension.services.schedule.ServiceTaskSchedulerImpl;
import org.matsim.contrib.drt.extension.services.services.params.DrtServicesParams;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilityFinder;
import org.matsim.contrib.drt.optimizer.DefaultDrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtRequestInsertionRetryQueue;
import org.matsim.contrib.drt.optimizer.depot.DepotFinder;
import org.matsim.contrib.drt.optimizer.insertion.UnplannedRequestInserter;
import org.matsim.contrib.drt.optimizer.rebalancing.RebalancingStrategy;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
import org.matsim.contrib.drt.scheduler.DrtScheduleInquiry;
import org.matsim.contrib.drt.scheduler.EmptyVehicleRelocator;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
import org.matsim.contrib.dvrp.schedule.ScheduleTimingUpdater;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.mobsim.framework.MobsimTimer;
import org.matsim.core.router.costcalculators.TravelDisutilityFactory;
import org.matsim.core.router.util.TravelTime;

/**
 * @author steffenaxer
 */
public class DrtServiceOptimizerQSimModule extends AbstractDvrpModeQSimModule {

	private final DrtServicesParams drtServicesParams;
	private final DrtConfigGroup drtConfigGroup;

	public DrtServiceOptimizerQSimModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.getMode());
		this.drtConfigGroup = drtConfigGroup;
		this.drtServicesParams = ((DrtWithExtensionsConfigGroup) drtConfigGroup).getServicesParams().orElseThrow();
	}

	@Override
	protected void configureQSim() {

		bindModal(ServiceTaskDispatcher.class).toProvider(modalProvider(getter -> new ServiceTaskDispatcherImpl(
			drtServicesParams,
			getter.getModal(Fleet.class),
			getter.get(EventsManager.class),
			getter.getModal(ServiceTaskScheduler.class),
			getter.getModal(OperationFacilityFinder.class),
			getter.getModal(ServiceTriggerFactory.class)))).asEagerSingleton();

		addModalComponent(DrtOptimizer.class, modalProvider(
			getter -> {
				var delegate = new DefaultDrtOptimizer(drtConfigGroup, getter.getModal(Fleet.class), getter.get(MobsimTimer.class),
					getter.getModal(DepotFinder.class), getter.getModal(RebalancingStrategy.class),
					getter.getModal(DrtScheduleInquiry.class), getter.getModal(ScheduleTimingUpdater.class),
					getter.getModal(EmptyVehicleRelocator.class), getter.getModal(UnplannedRequestInserter.class),
					getter.getModal(DrtRequestInsertionRetryQueue.class));
				return new ServiceTaskOptimizer(getter.getModal(ServiceTaskDispatcher.class),
					delegate,
					getter.getModal(ScheduleTimingUpdater.class));
			}));

		bindModal(ServiceTaskScheduler.class).toProvider(modalProvider(getter -> new ServiceTaskSchedulerImpl(
			getter.getModal(Network.class),
			getter.getModal(TravelTime.class),
			getter.getModal(TravelDisutilityFactory.class).createTravelDisutility(getter.getModal(TravelTime.class)),
			getter.get(MobsimTimer.class),
			getter.getModal(DrtTaskFactory.class)))).asEagerSingleton();
	}
}
