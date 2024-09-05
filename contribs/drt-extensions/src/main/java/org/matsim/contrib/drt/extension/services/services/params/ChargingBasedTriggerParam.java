package org.matsim.contrib.drt.extension.services.services.params;


/**
 * @author steffenaxer
 */
public class ChargingBasedTriggerParam extends AbstractServiceTriggerParam {
	public static final String SET_NAME = "ChargingBasedTrigger";

	public ChargingBasedTriggerParam() {
		super(SET_NAME);
	}

	@Comment("Offsetting service duration with previous charging task")
	@Parameter
	public boolean offsetWithPrevTask = true;
}
