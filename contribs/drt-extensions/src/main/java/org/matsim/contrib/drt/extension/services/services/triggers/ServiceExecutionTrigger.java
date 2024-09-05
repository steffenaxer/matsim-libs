package org.matsim.contrib.drt.extension.services.services.triggers;

import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public interface ServiceExecutionTrigger {
	boolean requiresService(DvrpVehicle dvrpVehicle, double timeStep);
	String getName();
}
