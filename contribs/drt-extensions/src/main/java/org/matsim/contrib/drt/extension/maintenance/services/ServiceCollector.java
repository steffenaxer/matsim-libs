package org.matsim.contrib.drt.extension.maintenance.services;

import java.util.List;

import org.matsim.contrib.drt.extension.maintenance.services.params.ServiceExecutionConfigGroup;

/**
 * @author steffenaxer
 */
public interface ServiceCollector {
	List<ServiceExecutionConfigGroup> getServices();
	void addService(ServiceExecutionConfigGroup serviceExecutionConfigGroup);
	void removeService(ServiceExecutionConfigGroup serviceExecutionConfigGroup);


}
