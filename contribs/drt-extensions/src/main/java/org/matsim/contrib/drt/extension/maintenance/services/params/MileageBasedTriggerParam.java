package org.matsim.contrib.drt.extension.maintenance.services.params;


/**
 * @author steffenaxer
 */
public class MileageBasedTriggerParam extends AbstractServiceTriggerParam {
	public static final String SET_NAME = "MileageBasedTrigger";

	public MileageBasedTriggerParam() {
		super(SET_NAME);
	}

	@Comment("Required mileage to dispatch maintenance")
	@Parameter
	public int requiredMileage = 200_000;
}
