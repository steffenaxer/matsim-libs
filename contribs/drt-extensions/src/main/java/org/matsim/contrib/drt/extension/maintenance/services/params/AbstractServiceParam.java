package org.matsim.contrib.drt.extension.maintenance.services.params;

import jakarta.validation.constraints.NotNull;
import org.matsim.core.config.ConfigGroup;
import org.matsim.core.config.ReflectiveConfigGroup;

/**
 * @author steffenaxer
 */
public abstract class AbstractServiceParam extends ReflectiveConfigGroup {

	@NotNull
	@Parameter
	public String name;

	public AbstractServiceParam(String name)
	{
		super(name);
		this.name = name;
	}

	@Override
	public void addParameterSet(ConfigGroup configGroup) {
		if (configGroup instanceof AbstractServiceParam) {
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
