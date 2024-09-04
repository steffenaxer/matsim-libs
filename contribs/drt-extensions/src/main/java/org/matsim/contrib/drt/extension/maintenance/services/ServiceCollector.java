package org.matsim.contrib.drt.extension.maintenance.services;

import java.util.List;

import org.matsim.contrib.drt.extension.maintenance.services.params.DrtServiceParams;

/**
 * @author steffenaxer
 */
public interface ServiceCollector {
	List<DrtServiceParams> getServices();
	void addService(DrtServiceParams drtServiceParams);
	void removeService(DrtServiceParams drtServiceParams);


}
