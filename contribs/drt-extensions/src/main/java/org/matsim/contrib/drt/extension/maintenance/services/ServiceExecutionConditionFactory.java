package org.matsim.contrib.drt.extension.maintenance.services;

import org.matsim.contrib.drt.extension.maintenance.services.conditions.ServiceExecutionCondition;
import org.matsim.contrib.drt.extension.maintenance.services.params.AbstractServiceParam;

/**
 * @author steffenaxer
 */
public interface ServiceExecutionConditionFactory {
	ServiceExecutionCondition get(AbstractServiceParam param);
}
