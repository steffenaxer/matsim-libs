package org.matsim.contrib.drt.run;

import org.matsim.core.controler.AbstractModule;

/**
 * @author steffenaxer
 */
public class DrtCompanionTripModule extends AbstractModule {
	@Override
	public void install() {
		addControlerListenerBinding().to(DrtCompanionTripGenerator.class);
	}
}
