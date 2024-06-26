/* *********************************************************************** *
 * project: org.matsim.*
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2012 by the members listed in the COPYING,        *
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
 * *********************************************************************** */

package org.matsim.contrib.locationchoice.frozenepsilons;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.contrib.locationchoice.frozenepsilons.DestinationChoiceContext.ActivityFacilityWithIndex;
import org.matsim.core.population.algorithms.PlanAlgorithm;
import org.matsim.core.replanning.modules.AbstractMultithreadedModule;
import org.matsim.facilities.ActivityFacility;

class ComputeMaxDCScoreMultiThreatedModule extends AbstractMultithreadedModule {
	
	private static final Logger log = LogManager.getLogger(ComputeMaxDCScoreMultiThreatedModule.class);
	
	private final String type;
	private final ActivityFacilityWithIndex[] typedFacilities;
	private final DestinationChoiceContext lcContext;
	private final DestinationSampler sampler;
		
	public ComputeMaxDCScoreMultiThreatedModule(String type, DestinationChoiceContext lcContext, DestinationSampler sampler) {
		super(lcContext.getScenario().getConfig().global().getNumberOfThreads());
		this.type = type;
		this.lcContext = lcContext;

		/*
		 * Get ActivityFacilies for type and then replace them with ActivityFacilityWithIndex
		 * objects due to performance reasons.
		 */
		Map<Id<ActivityFacility>, ActivityFacility> map = lcContext.getScenario().getActivityFacilities().getFacilitiesForActivityType( type );
		this.typedFacilities = new ActivityFacilityWithIndex[map.size()];
		int i = 0;
		for (ActivityFacility activityFacility : map.values()) {
			int index = this.lcContext.getFacilityIndex(activityFacility.getId());
			this.typedFacilities[i] = new ActivityFacilityWithIndex(activityFacility, index);
			i++;
		}
		
		if (this.typedFacilities.length == 0) {
			log.warn("There are no facilities for type : " + type);
		} 
		this.sampler = sampler;
	}

	@Override
	public PlanAlgorithm getPlanAlgoInstance() {
		DestinationScoring scorer = new DestinationScoring(this.lcContext);		
		return new ComputeMaxDCScorePlanAlgo(this.type, this.typedFacilities, scorer, this.sampler, this.lcContext);
	}
}
