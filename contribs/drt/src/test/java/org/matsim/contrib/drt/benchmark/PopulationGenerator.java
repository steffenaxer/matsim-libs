package org.matsim.contrib.drt.benchmark;

import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.population.*;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.Id;
import org.matsim.core.config.groups.ScoringConfigGroup;
import org.matsim.core.gbl.MatsimRandom;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PopulationGenerator {

	private static final Random random = MatsimRandom.getLocalInstance();

	public static void generatePopulation(int numberOfAgents, Scenario scenario) {
		Population population = scenario.getPopulation();
		List<Link> links = new ArrayList<>(scenario.getNetwork().getLinks().values());

		scenario.getConfig().scoring().addActivityParams(
			new ScoringConfigGroup.ActivityParams("home").setTypicalDuration(8*3600)
		);
		scenario.getConfig().scoring().addActivityParams(
			new ScoringConfigGroup.ActivityParams("work").setTypicalDuration(8*3600)
		);


		for (int i = 0; i < numberOfAgents; i++) {
			// Random origin and destination links
			Link originLink = links.get(random.nextInt(links.size()));
			Link destinationLink;
			do {
				destinationLink = links.get(random.nextInt(links.size()));
			} while (destinationLink.equals(originLink));

			// Random departure time (0 - 86400 seconds)
			double departureTime = random.nextDouble() * 86400;

			// Create person and plan
			Person person = population.getFactory().createPerson(Id.createPersonId("person_" + i));
			Plan plan = population.getFactory().createPlan();

			// Home activity on origin link
			Activity home = population.getFactory().createActivityFromLinkId("home", originLink.getId());
			home.setEndTime(departureTime);
			plan.addActivity(home);

			// DRT leg
			Leg leg = population.getFactory().createLeg("drt");
			plan.addLeg(leg);

			// Work activity on destination link
			Activity work = population.getFactory().createActivityFromLinkId("work", destinationLink.getId());
			plan.addActivity(work);

			person.addPlan(plan);
			population.addPerson(person);
		}
	}
}
