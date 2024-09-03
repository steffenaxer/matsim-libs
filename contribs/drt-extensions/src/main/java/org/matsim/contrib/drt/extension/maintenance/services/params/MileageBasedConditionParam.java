package org.matsim.contrib.drt.extension.maintenance.services.params;


/**
 * @author steffenaxer
 */
public class MileageBasedConditionParam extends AbstractServiceParam {
	public static final String SET_NAME = "MileageBasedCondition";

	public MileageBasedConditionParam() {
		super(SET_NAME);
	}

	@Comment("Required mileage to dispatch maintenance")
	@Parameter
	public int requiredMileage = 200_000;
}
