package org.matsim.contrib.drt.extension.maintenance.logic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author steffenaxer
 */
public class MaintenanceRuleCollectorImpl implements MaintenanceRulesCollector {
	List<MaintenanceRule> maintenanceRules = new ArrayList<>();;

	@Override
	public void installMaintenanceRule(MaintenanceRule maintenanceRule)
	{
		this.maintenanceRules.add(maintenanceRule);
	}

	@Override
	public List<MaintenanceRule> getMaintenanceRules() {
		return Collections.unmodifiableList(this.maintenanceRules);
	}
}
