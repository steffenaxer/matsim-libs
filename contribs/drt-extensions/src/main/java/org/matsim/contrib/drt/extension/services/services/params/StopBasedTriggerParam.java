package org.matsim.contrib.drt.extension.services.services.params;


/**
 * @author steffenaxer
 */
public class StopBasedTriggerParam extends AbstractServiceTriggerParam {
	public static final String SET_NAME = "StopBasedTrigger";

	public StopBasedTriggerParam() {
		super(SET_NAME);
	}

	@Comment("Required stops to dispatch service")
	@Parameter
	public int requiredStops = 50;
}
