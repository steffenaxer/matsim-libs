package org.matsim.contrib.drt.extension.maintenance.services;

import org.matsim.contrib.drt.extension.maintenance.services.params.*;
import org.matsim.contrib.drt.run.DrtConfigGroup;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeModule;
/**
 * @author steffenaxer
 */
public class ServiceExecutionModule extends AbstractDvrpModeModule {
	public ServiceExecutionModule(DrtConfigGroup drtConfigGroup) {
		super(drtConfigGroup.mode);
	}

	@Override
	public void install() {

		bindModal(ServiceExecutionConditionFactory.class).toInstance(new DefaultServiceExecutionConditionFactoryImpl());

		bindModal(ServiceCollector.class).toProvider(
				modalProvider(getter -> {
					ServiceCollector collector = new ServiceCollectorImpl();

					ServiceExecutionConfigGroup cleaning = new ServiceExecutionConfigGroup("clean");
					cleaning.maxRepetition = 2;
					cleaning.duration = 900;

					var condition1 = new ChargingBasedConditionParam();
					condition1.offsetWithPrevTask = true;

					var condition2 = new MileageBasedConditionParam();
					condition2.requiredMileage = 50_000;

					var condition3 = new StopBasedConditionParam();
					condition3.requiredStops = 50;

					cleaning.addParameterSet(condition1);
					cleaning.addParameterSet(condition2);
					cleaning.addParameterSet(condition3);

					collector.addService(cleaning);

					return collector;
				}))
			.asEagerSingleton();
	}
}
