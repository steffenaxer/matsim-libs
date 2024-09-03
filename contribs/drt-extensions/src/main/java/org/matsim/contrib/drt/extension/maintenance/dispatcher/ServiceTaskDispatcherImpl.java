package org.matsim.contrib.drt.extension.maintenance.dispatcher;

import com.google.common.base.Verify;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.maintenance.services.ServiceExecutionConditionFactory;
import org.matsim.contrib.drt.extension.maintenance.services.ServiceCollector;
import org.matsim.contrib.drt.extension.maintenance.services.conditions.ServiceExecutionCondition;
import org.matsim.contrib.drt.extension.maintenance.services.params.AbstractServiceParam;
import org.matsim.contrib.drt.extension.maintenance.services.params.ServiceExecutionConfigGroup;
import org.matsim.contrib.drt.extension.maintenance.schedule.ServiceTaskScheduler;
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

import java.util.Collection;

/**
 * @author steffenaxer
 */
public class ServiceTaskDispatcherImpl implements ServiceTaskDispatcher {
	private static final Logger LOG = LogManager.getLogger(ServiceTaskDispatcherImpl.class);
	public static final double CHECK_SERVICE_INTERVAL = 900.;
	public static final double MAINTENANCE_TIME = 900.;
	private final Fleet fleet;
	private final EventsManager eventsManager;
	private final ServiceTaskScheduler serviceTaskScheduler;
	private final ServiceCollector serviceCollector;
	private final OperationFacilityFinder operationFacilityFinder;
	private final ServiceExecutionConditionFactory serviceExecutionConditionFactory;

	public ServiceTaskDispatcherImpl(Fleet fleet, EventsManager eventsManager,
									 ServiceTaskScheduler serviceTaskScheduler,
									 ServiceCollector serviceCollector,
									 OperationFacilityFinder operationFacilityFinder,
									 ServiceExecutionConditionFactory serviceExecutionConditionFactory) {
		this.fleet = fleet;
		this.eventsManager = eventsManager;
		this.serviceTaskScheduler = serviceTaskScheduler;
		this.serviceCollector = serviceCollector;
		this.operationFacilityFinder = operationFacilityFinder;
		this.serviceExecutionConditionFactory = serviceExecutionConditionFactory;
	}

	@Override
	public void dispatch(double timeStep) {
		//TODO Move maintenance interval to config params
		//TODO Make this run parallel
		if (timeStep % CHECK_SERVICE_INTERVAL == 0) {
			for (DvrpVehicle dvrpVehicle : this.fleet.getVehicles().values()) {
				for (ServiceExecutionConfigGroup serviceExecutionConfigGroup : this.serviceCollector.getServices()) {
					String serviceName = serviceExecutionConfigGroup.name;
					Collection<? extends Collection<? extends ConfigGroup>> executionConditions = serviceExecutionConfigGroup.getParameterSets().values();
					for (Collection<? extends ConfigGroup> executionCondition : executionConditions) {
						Verify.verify(executionCondition.size() == 1);
						ServiceExecutionCondition serviceExecutionCondition = this.serviceExecutionConditionFactory.get((AbstractServiceParam) executionCondition.iterator().next());
						if (serviceExecutionCondition.requiresService(dvrpVehicle, timeStep)) {
							this.serviceTaskScheduler.scheduleServiceTask(dvrpVehicle, findServiceFacility(dvrpVehicle));
							break; // Only assign one maintenance task for each maintenance type
						}
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
