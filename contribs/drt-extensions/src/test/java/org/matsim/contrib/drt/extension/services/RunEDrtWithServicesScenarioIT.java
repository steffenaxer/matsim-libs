package org.matsim.contrib.drt.extension.services;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.edrt.optimizer.EDrtVehicleDataEntryFactory;
import org.matsim.contrib.drt.extension.edrt.run.EDrtControlerCreator;
import org.matsim.contrib.drt.extension.services.optimizer.EDrtServiceOptimizerQSimModule;
import org.matsim.contrib.drt.extension.services.optimizer.EDrtServiceQSimModule;
import org.matsim.contrib.drt.extension.services.services.ServiceExecutionModule;
import org.matsim.contrib.drt.extension.services.services.params.DrtServiceParams;
import org.matsim.contrib.drt.extension.services.services.params.DrtServicesParams;
import org.matsim.contrib.drt.extension.services.services.params.TimeOfDayBasedTriggerParam;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesModeModule;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesQSimModule;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeModule;
import org.matsim.contrib.ev.charging.*;
import org.matsim.contrib.ev.temperature.TemperatureService;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.controler.AbstractModule;
import org.matsim.core.controler.Controler;
import org.matsim.testcases.MatsimTestUtils;

public class RunEDrtWithServicesScenarioIT {
	@RegisterExtension
	private final MatsimTestUtils utils = new MatsimTestUtils();


    @Test
    void test() {
		final String outputDirectory = utils.getOutputDirectory();
		final Config config = ServicesTestUtils.configure(outputDirectory, true);
		var multiModeDrtConfigGroup = ConfigUtils.addOrGetModule(config, MultiModeDrtConfigGroup.class);
		var drtConfigGroup = multiModeDrtConfigGroup.getModalElements().stream().findFirst().orElseThrow();
		drtConfigGroup.idleVehiclesReturnToDepots = true; //Required for standard eDrt

		DrtServicesParams drtServicesParams = new DrtServicesParams();

		{
			DrtServiceParams clean = new DrtServiceParams("clean");
			clean.executionLimit = 1;
			clean.duration = 900;
			var condition1 = new TimeOfDayBasedTriggerParam();
			condition1.executionTime = 53205;
			clean.addParameterSet(condition1);
			drtServicesParams.addParameterSet(clean);
		}

		{
			DrtServiceParams deepClean = new DrtServiceParams("deep clean");
			deepClean.executionLimit = 1;
			deepClean.duration = 1800;
			var condition1 = new TimeOfDayBasedTriggerParam();
			condition1.executionTime = 53205;
			deepClean.addParameterSet(condition1);
			drtServicesParams.addParameterSet(deepClean);
		}

		drtConfigGroup.addParameterSet(drtServicesParams);

        final Controler run = EDrtControlerCreator.createControler(config,false);
		run.addOverridingModule(new OperationFacilitiesModeModule((DrtWithExtensionsConfigGroup) drtConfigGroup));
		run.addOverridingModule(new ServiceExecutionModule(drtConfigGroup));
		run.addOverridingQSimModule(new OperationFacilitiesQSimModule(drtConfigGroup));
		run.addOverridingQSimModule(new EDrtServiceQSimModule(drtConfigGroup));
		run.addOverridingQSimModule(new EDrtServiceOptimizerQSimModule(drtConfigGroup));

		run.addOverridingModule(new AbstractDvrpModeModule(drtConfigGroup.getMode()) {
			@Override
			public void install() {
				bind(EDrtVehicleDataEntryFactory.EDrtVehicleDataEntryFactoryProvider.class).toInstance(
					new EDrtVehicleDataEntryFactory.EDrtVehicleDataEntryFactoryProvider(0.2));
			}
		});

		run.addOverridingModule(new AbstractModule() {
			@Override
			public void install() {
				bind(ChargingLogic.Factory.class).toProvider(new ChargingWithQueueingAndAssignmentLogic.FactoryProvider(
					charger -> new ChargeUpToMaxSocStrategy(charger, 1.0)));
				bind(ChargingPower.Factory.class).toInstance(ev -> new FixedSpeedCharging(ev, 1.0));
				bind(TemperatureService.class).toInstance(linkId -> 20.);
			}
		});

        run.run();
    }

}
