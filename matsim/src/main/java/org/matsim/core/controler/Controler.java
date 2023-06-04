package org.matsim.core.controler;

import org.matsim.api.core.v01.Scenario;
import org.matsim.core.config.Config;

import java.util.Collections;
import java.util.List;

/**
 * @author steffenaxer
 */
public class Controler extends AbstractMATSimControler {
	public Controler(String[] args) {
		super(args);
	}

	public Controler(String configFileName) {
		super(configFileName);
	}

	public Controler(Config config) {
		super(config);
	}

	public Controler(Scenario scenario) {
		super(scenario);
	}

	@Override
	protected void loadDefaultModules(List<AbstractModule> modules) {
		modules.addAll(Collections.singletonList(new ControlerDefaultsModule()));
	}
}
