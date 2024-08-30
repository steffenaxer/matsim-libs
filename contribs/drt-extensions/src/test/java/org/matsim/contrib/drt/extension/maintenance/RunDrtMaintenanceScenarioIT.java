package org.matsim.contrib.drt.extension.maintenance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.matsim.contrib.drt.extension.DrtWithExtensionsConfigGroup;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesModeModule;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacilitiesQSimModule;
import org.matsim.contrib.drt.run.DrtControlerCreator;
import org.matsim.contrib.drt.run.MultiModeDrtConfigGroup;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeQSimModule;
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
		controler.addOverridingQSimModule(new OperationFacilitiesQSimModule(drtConfigGroup));
		controler.addOverridingQSimModule(new AbstractDvrpModeQSimModule(drtConfigGroup.getMode()) {
			@Override
			protected void configureQSim() {
				install(new DrtMaintenanceQSimModule(drtConfigGroup));
			}
		});

        controler.run();
    }

}
