package org.matsim.contrib.drt.extension.maintenance.dispatcher;

import com.google.common.base.Verify;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.maintenance.services.ServiceExecutionConditionFactory;
import org.matsim.contrib.drt.extension.maintenance.services.ServiceCollector;
import org.matsim.contrib.drt.extension.maintenance.services.conditions.ServiceExecutionCondition;
import org.matsim.contrib.drt.extension.maintenance.services.conditions.TimedCondition;
import org.matsim.contrib.drt.extension.maintenance.services.params.AbstractServiceParam;
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
	private final ServiceExecutionConditionFactory serviceExecutionConditionFactory;
	private final Map<AbstractServiceParam,ServiceExecutionCondition> serviceConditionMap;
	private final Set<Double> executionTimes = new HashSet<>();

	public ServiceTaskDispatcherImpl(final DrtServicesParams drtServicesParams, Fleet fleet, EventsManager eventsManager,
									 ServiceTaskScheduler serviceTaskScheduler,
									 ServiceCollector serviceCollector,
									 OperationFacilityFinder operationFacilityFinder,
									 ServiceExecutionConditionFactory serviceExecutionConditionFactory) {
		this.drtServicesParams = drtServicesParams;
		this.fleet = fleet;
		this.eventsManager = eventsManager;
		this.serviceTaskScheduler = serviceTaskScheduler;
		this.serviceCollector = serviceCollector;
		this.operationFacilityFinder = operationFacilityFinder;
		this.serviceExecutionConditionFactory = serviceExecutionConditionFactory;
		this.serviceConditionMap = createServiceConditions();
	}

	@Override
	public void dispatch(double timeStep) {
		//TODO Make this run parallel
		if (timeStep % drtServicesParams.executionInterval == 0 || this.executionTimes.contains(timeStep)) {
			for (DvrpVehicle dvrpVehicle : this.fleet.getVehicles().values()) {
				for (DrtServiceParams drtServiceParams : this.serviceCollector.getServices()) {
					double duration = drtServiceParams.duration;
					Collection<? extends Collection<? extends ConfigGroup>> executionConditions = drtServiceParams.getParameterSets().values();
					for (Collection<? extends ConfigGroup> executionCondition : executionConditions) {
						var condition = (AbstractServiceParam) executionCondition.iterator().next();
						ServiceExecutionCondition serviceExecutionCondition = serviceConditionMap.get(condition);
						if (serviceExecutionCondition.requiresService(dvrpVehicle, timeStep)) {
							this.serviceTaskScheduler.scheduleServiceTask(dvrpVehicle, findServiceFacility(dvrpVehicle), duration);
							break; // Only assign for first matching condition
						}
					}
				}
			}
		}
	}

	private Map<AbstractServiceParam, ServiceExecutionCondition> createServiceConditions() {
		Map<AbstractServiceParam, ServiceExecutionCondition> map = new LinkedHashMap<>();
		for (DrtServiceParams drtServiceParams : this.serviceCollector.getServices()) {
			Collection<? extends Collection<? extends ConfigGroup>> executionConditions = drtServiceParams.getParameterSets().values();
			for (Collection<? extends ConfigGroup> executionCondition : executionConditions) {
				Verify.verify(executionCondition.size() == 1);
				var service = (AbstractServiceParam) executionCondition.iterator().next();
				ServiceExecutionCondition serviceExecutionCondition = this.serviceExecutionConditionFactory.get(service);
				map.put(service, serviceExecutionCondition);
				if (serviceExecutionCondition instanceof TimedCondition timedCondition) {
					//Register explicit execution times
					this.executionTimes.add(timedCondition.getExecutionTime());
				}

			}
		}
		return map;
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
