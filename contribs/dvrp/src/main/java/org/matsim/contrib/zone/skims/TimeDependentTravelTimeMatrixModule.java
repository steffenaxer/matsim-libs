/*
 * *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2023 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** *
 */

package org.matsim.contrib.zone.skims;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeModule;
import org.matsim.contrib.dvrp.run.DvrpConfigGroup;
import org.matsim.core.config.groups.GlobalConfigGroup;
import org.matsim.core.config.groups.QSimConfigGroup;

/**
 * @author steffenaxer
 */
public class TimeDependentTravelTimeMatrixModule extends AbstractDvrpModeModule {
	@Inject
	private DvrpConfigGroup dvrpConfigGroup;
	@Inject
	private GlobalConfigGroup globalConfigGroup;
	@Inject
	private QSimConfigGroup qSimConfigGroup;

	public TimeDependentTravelTimeMatrixModule(String mode) {
		super(mode);
	}

	@Override
	public void install() {
		bindModal(TravelTimeMatrix.class).toProvider(modalProvider(
				getter -> TimeDependentTravelTimeMatrix.createMatrix(getter.getModal(Network.class),
						dvrpConfigGroup.getTravelTimeMatrixParams(), globalConfigGroup.getNumberOfThreads(),
						qSimConfigGroup.getTimeStepSize()))).in(Singleton.class);
		bindModal(TimeDependentMatrixUpdater.class).toProvider(modalProvider(
				getter -> new TimeDependentMatrixUpdater(getter.getModal(TravelTimeMatrix.class))));
		addControlerListenerBinding().to(modalKey(TimeDependentMatrixUpdater.class));
	}
}
