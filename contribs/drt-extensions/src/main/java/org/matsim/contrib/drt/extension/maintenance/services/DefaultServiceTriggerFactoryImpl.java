package org.matsim.contrib.drt.extension.maintenance.services;

import org.matsim.api.core.v01.Id;
import org.matsim.contrib.drt.extension.maintenance.services.triggers.*;
import org.matsim.contrib.drt.extension.maintenance.services.params.*;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;

/**
 * @author steffenaxer
 */
public class DefaultServiceTriggerFactoryImpl implements ServiceTriggerFactory {

	@Override
	public ServiceExecutionTrigger get(Id<DvrpVehicle> vehicleId, AbstractServiceTriggerParam param) {
		return switch (param) {
			case StopBasedTriggerParam stopBasedConditionParam -> new StopBasedServiceTrigger(vehicleId, stopBasedConditionParam);
			case MileageBasedTriggerParam mileageBasedConditionParam -> new MileageBasedServiceTrigger(vehicleId, mileageBasedConditionParam);
			case ChargingBasedTriggerParam chargingBasedConditionParam -> new ChargingBasedServiceTrigger(vehicleId,chargingBasedConditionParam);
			case TimeOfDayBasedTriggerParam timeOfDayBasedConditionParam -> new TimeOfDayBasedServiceTrigger(vehicleId, timeOfDayBasedConditionParam);
			default -> throw new IllegalStateException("JobConditionFactory missing for MaintenanceParam " + param.getName());
		};
	}
}
