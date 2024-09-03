package org.matsim.contrib.drt.extension.maintenance.services;

import org.matsim.contrib.drt.extension.maintenance.services.conditions.*;
import org.matsim.contrib.drt.extension.maintenance.services.params.*;

/**
 * @author steffenaxer
 */
public class DefaultServiceExecutionConditionFactoryImpl implements ServiceExecutionConditionFactory {

	@Override
	public ServiceExecutionCondition get(AbstractServiceParam param) {
		return switch (param) {
			case StopBasedConditionParam stopBasedConditionParam -> new StopBasedServiceExecutionCondition(stopBasedConditionParam);
			case MileageBasedConditionParam mileageBasedConditionParam -> new MileageBaseServiceExecutionCondition(mileageBasedConditionParam);
			case ChargingBasedConditionParam chargingBasedConditionParam -> new ChargingBasedServiceExecutionCondition(chargingBasedConditionParam);
			case TimeOfDayBasedConditionParam timeOfDayBasedConditionParam -> new TimeOfDayBaseServiceExecutionCondition(timeOfDayBasedConditionParam);
			default -> throw new IllegalStateException("JobConditionFactory missing for MaintenanceParam " + param.getName());
		};
	}
}
