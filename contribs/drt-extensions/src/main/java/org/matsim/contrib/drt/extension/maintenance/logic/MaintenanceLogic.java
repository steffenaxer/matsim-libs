package org.matsim.contrib.drt.extension.maintenance.logic;

import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.fleet.Fleet;

import java.util.List;

/**
 * @author steffenaxer
 */
public interface MaintenanceLogic {
	List<DvrpVehicle> requiresMaintenance(Fleet fleet, double timeStep);
}
