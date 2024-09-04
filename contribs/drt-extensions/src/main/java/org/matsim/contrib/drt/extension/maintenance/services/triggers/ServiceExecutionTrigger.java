package org.matsim.contrib.drt.extension.maintenance.services.triggers;

import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public interface ServiceExecutionTrigger {
	boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep);
}
