package org.matsim.contrib.drt.extension.maintenance.services.params;


/**
 * @author steffenaxer
 */
public class StopBasedConditionParam extends AbstractServiceParam {
	public static final String SET_NAME = "StopBasedCondition";

	public StopBasedConditionParam() {
		super(SET_NAME);
	}

	@Comment("Required stops to dispatch service")
	@Parameter
	public int requiredStops = 50;
}
