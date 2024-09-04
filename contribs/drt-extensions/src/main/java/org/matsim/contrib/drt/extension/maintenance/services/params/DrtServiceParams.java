package org.matsim.contrib.drt.extension.maintenance.services.params;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.matsim.core.config.ConfigGroup;
import org.matsim.core.config.ReflectiveConfigGroup;

/**
 * @author steffenaxer
 */
public class DrtServiceParams extends ReflectiveConfigGroup {
	public static final String SET_TYPE = "service";

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

	public DrtServiceParams() {
		this(null);
	}

	public DrtServiceParams(String name)
	{
		super(SET_TYPE);
		this.name = name;
	}

	@Override
	public ConfigGroup createParameterSet(final String type) {
		if (type.equals(SET_TYPE))
		{
			return new DrtServiceParams();
		}
		throw new IllegalStateException("Unsupported ConfigGroup "+ type);
	}


	@Override
	public void addParameterSet(ConfigGroup configGroup) {
		if (configGroup instanceof AbstractServiceParam) {
			super.addParameterSet(configGroup);
		} else {
			throw new IllegalStateException("Unsupported ConfigGroup "+ configGroup.getName());
		}
	}
}
