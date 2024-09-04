package org.matsim.contrib.drt.extension.maintenance.services;

import java.util.*;
import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServiceParams;

/**
 * @author steffenaxer
 */
public class ServiceCollectorImpl implements ServiceCollector {
	List<DrtServiceParams> maintenanceJobs = new ArrayList<>();;

	@Override
	public void addService(DrtServiceParams drtServiceParams)
	{
		this.maintenanceJobs.add(drtServiceParams);
	}

	@Override
	public void removeService(DrtServiceParams drtServiceParams) {
		maintenanceJobs.remove(drtServiceParams);
	}

	@Override
	public List<DrtServiceParams> getServices() {
		return Collections.unmodifiableList(this.maintenanceJobs);
	}
}
