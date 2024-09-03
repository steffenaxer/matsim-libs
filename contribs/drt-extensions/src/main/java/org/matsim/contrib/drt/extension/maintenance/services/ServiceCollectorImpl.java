package org.matsim.contrib.drt.extension.maintenance.services;

import java.util.*;
import org.matsim.contrib.drt.extension.maintenance.services.params.ServiceExecutionConfigGroup;

/**
 * @author steffenaxer
 */
public class ServiceCollectorImpl implements ServiceCollector {
	List<ServiceExecutionConfigGroup> maintenanceJobs = new ArrayList<>();;

	@Override
	public void addService(ServiceExecutionConfigGroup serviceExecutionConfigGroup)
	{
		this.maintenanceJobs.add(serviceExecutionConfigGroup);
	}

	@Override
	public void removeService(ServiceExecutionConfigGroup serviceExecutionConfigGroup) {
		maintenanceJobs.remove(serviceExecutionConfigGroup);
	}

	@Override
	public List<ServiceExecutionConfigGroup> getServices() {
		return Collections.unmodifiableList(this.maintenanceJobs);
	}
}
