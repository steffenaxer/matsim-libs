package org.matsim.contrib.drt.extension.services.services.params;


import jakarta.validation.constraints.NotNull;

/**
 * @author steffenaxer
 */
public class TimeOfDayBasedTriggerParam extends AbstractServiceTriggerParam implements TimedTriggerParam {
	public static final String SET_NAME = "TimeOfDayTrigger";

	public TimeOfDayBasedTriggerParam() {
		super(SET_NAME);
	}

	@NotNull
	@Comment("Execution time of the service")
	@Parameter
	public double executionTime;

	@Override
	public double getExecutionTime() {
		return executionTime;
	}
}
