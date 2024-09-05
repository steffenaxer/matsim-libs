package org.matsim.contrib.drt.extension.services.services;

import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.services.services.params.*;
import org.matsim.contrib.drt.extension.services.tasks.DefaultJoinableTasksImpl;
import org.matsim.contrib.drt.extension.services.tasks.JoinableTasks;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeModule;

/**
 * @author steffenaxer
 */
public class ServiceExecutionModule extends AbstractDvrpModeModule {
	private final DrtServicesParams drtServicesParams;

	public ServiceExecutionModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.mode);
		this.drtServicesParams = ((DrtWithExtensionsConfigGroup) drtConfigGroup).getServicesParams().orElseThrow();
	}

	@Override
	public void install() {
		bindModal(ServiceTriggerFactory.class).toInstance(new DefaultServiceTriggerFactoryImpl());
		bindModal(JoinableTasks.class).toInstance(new DefaultJoinableTasksImpl());
		}
}
