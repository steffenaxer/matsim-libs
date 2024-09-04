package org.matsim.contrib.drt.extension.maintenance.services;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.maintenance.services.triggers.AbstractTrigger;
import org.matsim.contrib.drt.extension.maintenance.services.params.AbstractServiceTriggerParam;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public interface ServiceTriggerFactory {
	AbstractTrigger get(Id<DvrpVehicle> vehicleId, AbstractServiceTriggerParam param);
}
