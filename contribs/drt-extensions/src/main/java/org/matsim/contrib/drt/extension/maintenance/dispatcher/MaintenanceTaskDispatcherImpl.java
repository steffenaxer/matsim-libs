package org.matsim.contrib.drt.extension.maintenance.dispatcher;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.maintenance.logic.MaintenanceLogic;
import org.matsim.contrib.drt.extension.maintenance.scheduler.MaintenanceTaskScheduler;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilities;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilityType;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.facilities.Facility;

import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author steffenaxer
 */
public class MaintenanceTaskDispatcherImpl implements MaintenanceTaskDispatcher {
	public static final double MAINTENANCE_TIME = 900.;
	private final Fleet fleet;
	private final EventsManager eventsManager;
	private final Map<Id<Link>, OperationFacility> hubLocations;
	private final MaintenanceTaskScheduler maintenanceTaskScheduler;
	private final MaintenanceLogic maintenanceLogic;

	public MaintenanceTaskDispatcherImpl(Fleet fleet, EventsManager eventsManager, OperationFacilities operationFacilities,
										 MaintenanceTaskScheduler maintenanceTaskScheduler, MaintenanceLogic maintenanceLogic) {
		this.fleet = fleet;
		this.eventsManager = eventsManager;
		this.hubLocations = operationFacilities.getDrtOperationFacilities().values().stream()
			.filter(o -> o.getType().equals(OperationFacilityType.hub))
			.collect(Collectors.toMap(Facility::getLinkId, o -> o));
		this.maintenanceTaskScheduler = maintenanceTaskScheduler;
		this.maintenanceLogic = maintenanceLogic;
	}

	@Override
	public void dispatch(double timeStep) {
		for (DvrpVehicle dvrpVehicle : maintenanceLogic.requiresMaintenance(fleet, timeStep)) {
			this.maintenanceTaskScheduler.relocateForMaintenance(dvrpVehicle, getRandomHub());
		}
	}

	// TODO Replace with DepotFinder
	OperationFacility getRandomHub() {
		var hubs = this.hubLocations.values();
		return hubs.stream()
			.skip((int) (hubs.size() * MatsimRandom.getRandom().nextDouble()))
			.findFirst().orElseThrow();

	}
}
