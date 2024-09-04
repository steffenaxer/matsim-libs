package org.matsim.contrib.drt.extension.maintenance.dispatcher;

import com.google.common.base.Verify;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.maintenance.services.ServiceTriggerFactory;
import org.matsim.contrib.drt.extension.maintenance.services.ServiceCollector;
import org.matsim.contrib.drt.extension.maintenance.services.params.TimedTriggerParam;
import org.matsim.contrib.drt.extension.maintenance.services.triggers.AbstractTrigger;
import org.matsim.contrib.drt.extension.maintenance.services.params.AbstractServiceTriggerParam;
import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServiceParams;
import org.matsim.contrib.drt.extension.maintenance.schedule.ServiceTaskScheduler;
import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServicesParams;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilityFinder;
import org.matsim.contrib.drt.schedule.DrtStayTask;
import org.matsim.contrib.drt.scheduler.EmptyVehicleRelocator;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.contrib.dvrp.schedule.DriveTask;
import org.matsim.contrib.dvrp.schedule.Schedule;
import org.matsim.contrib.dvrp.schedule.Task;
import org.matsim.contrib.dvrp.tracker.OnlineDriveTaskTracker;
import org.matsim.contrib.dvrp.util.LinkTimePair;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.config.ConfigGroup;

import java.util.*;

/**
 * @author steffenaxer
 */
public class ServiceTaskDispatcherImpl implements ServiceTaskDispatcher {
	private static final Logger LOG = LogManager.getLogger(ServiceTaskDispatcherImpl.class);
	private final DrtServicesParams drtServicesParams;
	private final Fleet fleet;
	private final EventsManager eventsManager;
	private final ServiceTaskScheduler serviceTaskScheduler;
	private final ServiceCollector serviceCollector;
	private final OperationFacilityFinder operationFacilityFinder;
	private final ServiceTriggerFactory serviceTriggerFactory;
	private final Map<TriggerKey, AbstractTrigger> serviceConditionMap = new HashMap<>();
	private final Set<Double> timedTriggers = new HashSet<>();

	public ServiceTaskDispatcherImpl(final DrtServicesParams drtServicesParams, Fleet fleet, EventsManager eventsManager,
									 ServiceTaskScheduler serviceTaskScheduler,
									 ServiceCollector serviceCollector,
									 OperationFacilityFinder operationFacilityFinder,
									 ServiceTriggerFactory serviceTriggerFactory) {
		this.drtServicesParams = drtServicesParams;
		this.fleet = fleet;
		this.eventsManager = eventsManager;
		this.serviceTaskScheduler = serviceTaskScheduler;
		this.serviceCollector = serviceCollector;
		this.operationFacilityFinder = operationFacilityFinder;
		this.serviceTriggerFactory = serviceTriggerFactory;
		this.registerTimedTriggers();
	}

	@Override
	public void dispatch(double timeStep) {
		//TODO Make this run parallel
		if (timeStep % drtServicesParams.executionInterval == 0 || this.timedTriggers.contains(timeStep)) {
			for (DvrpVehicle dvrpVehicle : this.fleet.getVehicles().values()) {
				for (DrtServiceParams drtServiceParams : this.serviceCollector.getServices()) {
					String serviceName = drtServiceParams.name;
					double duration = drtServiceParams.duration;

					Collection<? extends Collection<? extends ConfigGroup>> executionConditions = drtServiceParams.getParameterSets().values();
					for (Collection<? extends ConfigGroup> executionCondition : executionConditions) {
						var condition = (AbstractServiceTriggerParam) executionCondition.iterator().next();
						TriggerKey key = new TriggerKey(dvrpVehicle.getId(), condition);
						AbstractTrigger abstractTrigger = serviceConditionMap
							.computeIfAbsent(key, k -> this.serviceTriggerFactory.get(dvrpVehicle.getId(), condition));
						if (abstractTrigger.requiresService(dvrpVehicle, timeStep)) {
							this.serviceTaskScheduler.scheduleServiceTask(dvrpVehicle, findServiceFacility(dvrpVehicle), duration);
							abstractTrigger.incrementTrigger();
							LOG.info("Triggered service {} for vehicle {} issued by {}", serviceName, dvrpVehicle.getId(), condition.name);
							break; // Only assign for first matching condition
						}
					}
				}
			}
		}
	}

	record TriggerKey(Id<DvrpVehicle> vehicleId, AbstractServiceTriggerParam abstractServiceTriggerParam) {
	}

	private void registerTimedTriggers() {
		for (DrtServiceParams drtServiceParams : this.serviceCollector.getServices()) {
			Collection<? extends Collection<? extends ConfigGroup>> executionConditions = drtServiceParams.getParameterSets().values();
			for (Collection<? extends ConfigGroup> executionCondition : executionConditions) {
				Verify.verify(executionCondition.size() == 1);
				var service = (AbstractServiceTriggerParam) executionCondition.iterator().next();

				if (service instanceof TimedTriggerParam timedTrigger) {
					//Register execution times from params
					LOG.info("Timed trigger {} for service {} with execution time {} registered", service.name, drtServiceParams.name, timedTrigger.getExecutionTime());
					this.timedTriggers.add(timedTrigger.getExecutionTime());
				}
			}
		}
	}

	private OperationFacility findServiceFacility(DvrpVehicle dvrpVehicle) {
		final Schedule schedule = dvrpVehicle.getSchedule();
		Task currentTask = schedule.getCurrentTask();
		Link lastLink;
		if (currentTask instanceof DriveTask
			&& currentTask.getTaskType().equals(EmptyVehicleRelocator.RELOCATE_VEHICLE_TASK_TYPE)
			&& currentTask.equals(schedule.getTasks().get(schedule.getTaskCount() - 2))) {
			LinkTimePair start = ((OnlineDriveTaskTracker) currentTask.getTaskTracker()).getDiversionPoint();
			if (start != null) {
				lastLink = start.link;
			} else {
				lastLink = ((DriveTask) currentTask).getPath().getToLink();
			}
		} else {
			lastLink = ((DrtStayTask) schedule.getTasks()
				.get(schedule.getTaskCount() - 1)).getLink();
		}
		return operationFacilityFinder.findFacility(lastLink.getCoord()).orElseThrow();
	}

}
