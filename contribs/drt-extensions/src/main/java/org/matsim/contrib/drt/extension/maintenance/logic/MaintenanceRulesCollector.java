package org.matsim.contrib.drt.extension.maintenance.logic;

import java.util.List;

/**
 * @author steffenaxer
 */
public interface MaintenanceRulesCollector {
	List<MaintenanceRule> getMaintenanceRules();
	void installMaintenanceRule(MaintenanceRule maintenanceRule);
}
