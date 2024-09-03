package org.matsim.contrib.drt.extension.maintenance.services.params;


import jakarta.validation.constraints.NotNull;

/**
 * @author steffenaxer
 */
public class TimeOfDayBasedConditionParam extends AbstractServiceParam {
	public static final String SET_NAME = "TimeOfDayConditionCondition";

	public TimeOfDayBasedConditionParam() {
		super(SET_NAME);
	}

	@NotNull
	@Comment("Time of day to dispatch service")
	@Parameter
	public double serviceTimeOfDay;
}
