package org.matsim.contrib.drt.extension.services.dispatcher;

import com.google.common.base.Verify;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.services.services.ServiceTriggerFactory;
import org.matsim.contrib.drt.extension.services.services.params.TimedTriggerParam;
import org.matsim.contrib.drt.extension.services.services.params.AbstractServiceTriggerParam;
import org.matsim.contrib.drt.extension.services.services.params.DrtServiceParams;
import org.matsim.contrib.drt.extension.services.schedule.ServiceTaskScheduler;
import org.matsim.contrib.drt.extension.services.services.params.DrtServicesParams;
import org.matsim.contrib.drt.extension.services.services.triggers.ServiceExecutionTracker;
import org.matsim.contrib.drt.extension.services.services.triggers.ServiceExecutionTrigger;
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
	private final OperationFacilityFinder operationFacilityFinder;
	private final ServiceTriggerFactory serviceTriggerFactory;
	private final Map<Id<DvrpVehicle>, ServiceExecutionTracker> executors = new HashMap<>();
	private final Set<Double> timedTriggers = new HashSet<>();

	public ServiceTaskDispatcherImpl(final DrtServicesParams drtServicesParams,
									 final Fleet fleet,
									 final EventsManager eventsManager,
									 final ServiceTaskScheduler serviceTaskScheduler,
									 final OperationFacilityFinder operationFacilityFinder,
									 final ServiceTriggerFactory serviceTriggerFactory) {
		this.drtServicesParams = drtServicesParams;
		this.fleet = fleet;
		this.eventsManager = eventsManager;
		this.serviceTaskScheduler = serviceTaskScheduler;
		this.operationFacilityFinder = operationFacilityFinder;
		this.serviceTriggerFactory = serviceTriggerFactory;
		this.registerServiceExecutors();
	}

	@Override
	public void dispatch(double timeStep) {
		if (timeStep % drtServicesParams.executionInterval == 0 || this.timedTriggers.contains(timeStep)) {
			for (DvrpVehicle dvrpVehicle : this.fleet.getVehicles().values()) {
				ServiceExecutionTracker tracker = this.executors.get(dvrpVehicle.getId());
				for (DrtServiceParams drtServiceParams : tracker.getServices()) {
					int executionLimit = drtServiceParams.executionLimit;
					int currentExecutions = tracker.getTriggerCounter(drtServiceParams);

					if(currentExecutions == executionLimit)
					{
						LOG.debug("Execution limit for vehicle {} and service {} reached.", drtServiceParams.name, dvrpVehicle.getId());
						continue;
					}

					for (ServiceExecutionTrigger serviceExecutionTrigger : tracker.getTriggers(drtServiceParams)) {
						if (serviceExecutionTrigger.requiresService(dvrpVehicle, timeStep)) {
							this.serviceTaskScheduler.scheduleServiceTask(dvrpVehicle, findServiceFacility(dvrpVehicle), drtServiceParams);
							tracker.TriggerCounter(drtServiceParams);
							LOG.debug("{} executed service {} for vehicle {} at {}.", serviceExecutionTrigger.getName(), drtServiceParams.name, dvrpVehicle.getId(), timeStep);
							break;
						}
					}
				}
			}
		}
	}

	private void registerServiceExecutors() {
		for (DvrpVehicle vehicle : this.fleet.getVehicles().values()) {
			ServiceExecutionTracker serviceExecutionTracker = new ServiceExecutionTracker(vehicle.getId());
			this.executors.put(vehicle.getId(), serviceExecutionTracker);
			for (ConfigGroup configGroup : this.drtServicesParams.getParameterSets(DrtServiceParams.SET_TYPE)) {
				DrtServiceParams drtServiceParams = (DrtServiceParams) configGroup;
				Collection<? extends Collection<? extends ConfigGroup>> executionConditions = drtServiceParams.getParameterSets().values();
				for (Collection<? extends ConfigGroup> executionCondition : executionConditions) {
					Verify.verify(executionCondition.size() == 1);
					var trigger = (AbstractServiceTriggerParam) executionCondition.iterator().next();
					serviceExecutionTracker.addTrigger(drtServiceParams, this.serviceTriggerFactory.get(vehicle.getId(), trigger));
					if (trigger instanceof TimedTriggerParam timedTrigger) {
						//Register execution times from params
						LOG.info("Timed trigger {} for service {} with execution time {} registered", trigger.name, drtServiceParams.name, timedTrigger.getExecutionTime());
						this.timedTriggers.add(timedTrigger.getExecutionTime());
					}
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
