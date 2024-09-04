package org.matsim.contrib.drt.extension.maintenance.services;

import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.maintenance.services.params.*;
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

		bindModal(ServiceCollector.class).toProvider(
				modalProvider(getter -> {
					ServiceCollector collector = new ServiceCollectorImpl();

					for (var service : drtServicesParams.getParameterSets(DrtServiceParams.SET_TYPE)) {
						collector.addService((DrtServiceParams) service);
					}

					return collector;
				}))
			.asEagerSingleton();
	}
}
