package org.matsim.contrib.drt.extension.maintenance.services.params;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.matsim.core.config.ConfigGroup;
import org.matsim.core.config.ReflectiveConfigGroup;

/**
 * @author steffenaxer
 */
public class ServiceExecutionConfigGroup extends ReflectiveConfigGroup {
	public static final String SET_TYPE = "serviceCollection";

	@NotNull
	@Parameter
	public String name;

	@NotNull
	@Positive
	@Parameter
	public double duration;

	@Positive
	@Parameter
	public int maxRepetition = Integer.MAX_VALUE;

	public ServiceExecutionConfigGroup() {
		this(null);
	}

	public ServiceExecutionConfigGroup(String name)
	{
		super(SET_TYPE);
		this.name = name;
	}

	@Override
	public ConfigGroup createParameterSet(final String type) {
		if (type.equals(SET_TYPE))
		{
			return new ServiceExecutionConfigGroup();
		}
		return new ConfigGroup( type );
	}


	@Override
	public void addParameterSet(ConfigGroup configGroup) {
		if (configGroup instanceof AbstractServiceParam) {
			super.addParameterSet(configGroup);
		} else {
			throw new IllegalArgumentException();
		}
	}
}
