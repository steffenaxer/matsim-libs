package org.matsim.contrib.drt.extension.services.services;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.services.services.params.AbstractServiceTriggerParam;
import org.matsim.contrib.drt.extension.services.services.triggers.ServiceExecutionTrigger;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public interface ServiceTriggerFactory {
	ServiceExecutionTrigger get(Id<DvrpVehicle> vehicleId, AbstractServiceTriggerParam param);
}
