package org.matsim.contrib.drt.extension.services;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.services.optimizer.DrtServiceOptimizerQSimModule;
import org.matsim.contrib.drt.extension.services.optimizer.DrtServiceQSimModule;
import org.matsim.contrib.drt.extension.services.services.ServiceExecutionModule;
import org.matsim.contrib.drt.extension.services.services.params.DrtServiceParams;
import org.matsim.contrib.drt.extension.services.services.params.DrtServicesParams;
import org.matsim.contrib.drt.extension.services.services.params.TimeOfDayBasedTriggerParam;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesModeModule;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesQSimModule;
import org.matsim.contrib.drt.run.DrtControlerCreator;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.controler.Controler;
import org.matsim.testcases.MatsimTestUtils;

public class RunDrtWithServicesScenarioIT {
	@RegisterExtension
	private final MatsimTestUtils utils = new MatsimTestUtils();

    @Test
    void test() {
		final String outputDirectory = utils.getOutputDirectory();
		final Config config = ServicesTestUtils.configure(outputDirectory, false);
        final Controler controler = DrtControlerCreator.createControler(config,false);
		var multiModeDrtConfigGroup = ConfigUtils.addOrGetModule(config, MultiModeDrtConfigGroup.class);
		var drtConfigGroup = multiModeDrtConfigGroup.getModalElements().stream().findFirst().orElseThrow();

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

		controler.addOverridingModule(new OperationFacilitiesModeModule((DrtWithExtensionsConfigGroup) drtConfigGroup));
		controler.addOverridingModule(new ServiceExecutionModule(drtConfigGroup));
		controler.addOverridingQSimModule(new OperationFacilitiesQSimModule(drtConfigGroup));
		controler.addOverridingQSimModule(new DrtServiceQSimModule(drtConfigGroup));
		controler.addOverridingQSimModule(new DrtServiceOptimizerQSimModule(drtConfigGroup));

        controler.run();
    }

}
