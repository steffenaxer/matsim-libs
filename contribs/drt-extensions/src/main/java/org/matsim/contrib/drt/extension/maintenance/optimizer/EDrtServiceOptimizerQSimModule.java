package org.matsim.contrib.drt.extension.maintenance.optimizer;

import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.edrt.optimizer.EDrtOptimizer;
import org.matsim.contrib.drt.extension.edrt.scheduler.EmptyVehicleChargingScheduler;
import org.matsim.contrib.drt.extension.maintenance.dispatcher.ServiceTaskDispatcher;
import org.matsim.contrib.drt.extension.maintenance.dispatcher.ServiceTaskDispatcherImpl;
import org.matsim.contrib.drt.extension.maintenance.services.*;
import org.matsim.contrib.drt.extension.maintenance.schedule.ServiceTaskScheduler;
import org.matsim.contrib.drt.extension.maintenance.schedule.ServiceTaskSchedulerImpl;
import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServicesParams;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilityFinder;
import org.matsim.contrib.drt.optimizer.DefaultDrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtOptimizer;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.drt.schedule.DrtTaskFactory;
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
public class EDrtServiceOptimizerQSimModule extends AbstractDvrpModeQSimModule {

	private final DrtServicesParams drtServicesParams;
	private final DrtConfigGroup drtConfigGroup;

	public EDrtServiceOptimizerQSimModule(DrtConfigGroup drtConfigGroup) {
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
			getter.getModal(ServiceCollector.class),
			getter.getModal(OperationFacilityFinder.class),
			getter.getModal(ServiceExecutionConditionFactory.class)))).asEagerSingleton();

		addModalComponent(DrtOptimizer.class, modalProvider(
			getter -> {
				var delegate =  new EDrtOptimizer(drtConfigGroup, getter.getModal(DefaultDrtOptimizer.class),
				getter.getModal(EmptyVehicleChargingScheduler.class));
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
