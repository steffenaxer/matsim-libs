package org.matsim.contrib.drt.extension.services.services.params;

import org.matsim.core.config.ConfigGroup;
import org.matsim.core.config.ReflectiveConfigGroup;

/**
 * @author steffenaxer
 */
public class DrtServicesParams extends ReflectiveConfigGroup {
	public static final String SET_TYPE = "services";

	@Comment("time interval to checked for execution in [seconds], except TimedCondition")
	@Parameter
	public double executionInterval = 600;

	public DrtServicesParams()
	{
		super(SET_TYPE);
	}

	@Override
	public ConfigGroup createParameterSet(final String type) {
		if (type.equals(SET_TYPE))
		{
			return new DrtServicesParams();
		}
		throw new IllegalStateException("Unsupported ConfigGroup "+ type);
	}


	@Override
	public void addParameterSet(ConfigGroup configGroup) {
		if (configGroup instanceof DrtServiceParams) {
			super.addParameterSet(configGroup);
		} else {
			throw new IllegalArgumentException("Unsupported ConfigGroup "+ configGroup.getName());
		}
	}
}
