package org.matsim.contrib.drt.extension.maintenance.logic;

import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public interface MaintenanceRule {
	boolean requiresMaintenance(DvrpVehicle dvrpVehicle, double timeStep);
}
