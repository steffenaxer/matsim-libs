package org.matsim.contrib.drt.extension.maintenance.services.triggers;

/**
 * @author steffenaxer
 */
public interface TriggerCounter {
	void incrementTrigger();
	int getTriggerCount();
}
