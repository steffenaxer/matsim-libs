package org.matsim.contrib.drt.extension.maintenance.services.params;


/**
 * @author steffenaxer
 */
public class ChargingBasedConditionParam extends AbstractServiceParam {
	public static final String SET_NAME = "ChargingBasedCondition";

	public ChargingBasedConditionParam() {
		super(SET_NAME);
	}

	@Comment("Offsetting service duration with previous charging task")
	@Parameter
	public boolean offsetWithPrevTask = true;
}
