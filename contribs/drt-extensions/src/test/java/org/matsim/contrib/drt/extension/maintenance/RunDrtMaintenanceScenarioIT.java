package org.matsim.contrib.drt.extension.maintenance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.maintenance.optimizer.DrtServiceOptimizerQSimModule;
import org.matsim.contrib.drt.extension.maintenance.optimizer.DrtServiceQSimModule;
import org.matsim.contrib.drt.extension.maintenance.services.ServiceExecutionModule;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesModeModule;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesQSimModule;
import org.matsim.contrib.drt.run.DrtControlerCreator;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.controler.Controler;
import org.matsim.testcases.MatsimTestUtils;

public class RunDrtMaintenanceScenarioIT {
	@RegisterExtension
	private final MatsimTestUtils utils = new MatsimTestUtils();

    @Test
    void test() {
		final String outputDirectory = utils.getOutputDirectory();
		final Config config = MaintenanceTestUtils.configure(outputDirectory);
        final Controler controler = DrtControlerCreator.createControler(config,false);
		var multiModeDrtConfigGroup = ConfigUtils.addOrGetModule(config, MultiModeDrtConfigGroup.class);
		var drtConfigGroup = multiModeDrtConfigGroup.getModalElements().stream().findFirst().orElseThrow();

		controler.addOverridingModule(new OperationFacilitiesModeModule((DrtWithExtensionsConfigGroup) drtConfigGroup));
		controler.addOverridingModule(new ServiceExecutionModule(drtConfigGroup));
		controler.addOverridingQSimModule(new OperationFacilitiesQSimModule(drtConfigGroup));
		controler.addOverridingQSimModule(new DrtServiceQSimModule(drtConfigGroup));
		controler.addOverridingQSimModule(new DrtServiceOptimizerQSimModule(drtConfigGroup));

        controler.run();
    }

}
