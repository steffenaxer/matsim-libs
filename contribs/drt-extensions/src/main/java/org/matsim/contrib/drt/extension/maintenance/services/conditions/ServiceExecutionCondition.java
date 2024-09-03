package org.matsim.contrib.drt.extension.maintenance.services.conditions;

import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public interface ServiceExecutionCondition {
	boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep);
}
