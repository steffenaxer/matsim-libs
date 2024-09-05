package org.matsim.contrib.drt.extension.services.services.params;

import jakarta.validation.constraints.NotNull;
import org.matsim.core.config.ConfigGroup;
import org.matsim.core.config.ReflectiveConfigGroup;

/**
 * @author steffenaxer
 */
public abstract class AbstractServiceTriggerParam extends ReflectiveConfigGroup {

	@NotNull
	@Parameter
	public String name;

	public AbstractServiceTriggerParam(String name)
	{
		super(name);
		this.name = name;
	}

	@Override
	public void addParameterSet(ConfigGroup configGroup) {
		if (configGroup instanceof AbstractServiceTriggerParam) {
			if(!this.getParameterSets().isEmpty())
			{
				throw new IllegalStateException("Adding more than one parameter is not allowed.");
			}
			super.addParameterSet(configGroup);
		} else {
			throw new IllegalArgumentException();
		}
	}

}
