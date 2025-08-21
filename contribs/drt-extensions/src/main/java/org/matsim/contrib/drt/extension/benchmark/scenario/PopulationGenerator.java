package org.matsim.contrib.drt.extension.benchmark.scenario;

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

	private static double sampleDepartureTime(boolean timeDependent) {
		if (!timeDependent) {
			return random.nextDouble() * 86400;
		}


		double morningPeak = gaussianSample(8 * 3600, 1.5 * 3600);
		double eveningPeak = gaussianSample(17 * 3600, 2 * 3600);

		return random.nextDouble() < 0.7 ? morningPeak : eveningPeak;
	}

	private static double gaussianSample(double mean, double stdDev) {
		double sample;
		do {
			sample = mean + random.nextGaussian() * stdDev;
		} while (sample < 0 || sample > 86400);
		return sample;
	}


	public static void generatePopulation(int numberOfAgents, Scenario scenario, boolean timeDependent) {
		Population population = scenario.getPopulation();
		List<Link> links = new ArrayList<>(scenario.getNetwork().getLinks().values());


		double centerX = 0;
		double centerY = 0;
		for (Link link : links) {
			centerX += link.getCoord().getX();
			centerY += link.getCoord().getY();
		}
		centerX /= links.size();
		centerY /= links.size();


		double sigma = 5000;
		double[] weights = new double[links.size()];
		double totalWeight = 0;
		for (int i = 0; i < links.size(); i++) {
			Link link = links.get(i);
			double dx = link.getCoord().getX() - centerX;
			double dy = link.getCoord().getY() - centerY;
			double distanceSquared = dx * dx + dy * dy;
			weights[i] = Math.exp(-distanceSquared / (2 * sigma * sigma));
			totalWeight += weights[i];
		}


		double[] cumulative = new double[weights.length];
		double sum = 0;
		for (int i = 0; i < weights.length; i++) {
			sum += weights[i] / totalWeight;
			cumulative[i] = sum;
		}


		java.util.function.Supplier<Link> weightedRandomLink = () -> {
			double r = random.nextDouble();
			for (int i = 0; i < cumulative.length; i++) {
				if (r <= cumulative[i]) return links.get(i);
			}
			return links.get(links.size() - 1); // fallback
		};


		scenario.getConfig().scoring().addActivityParams(
			new ScoringConfigGroup.ActivityParams("home").setTypicalDuration(8 * 3600)
		);
		scenario.getConfig().scoring().addActivityParams(
			new ScoringConfigGroup.ActivityParams("work").setTypicalDuration(8 * 3600)
		);


		for (int i = 0; i < numberOfAgents; i++) {
			Link originLink = weightedRandomLink.get();
			Link destinationLink;
			do {
				destinationLink = weightedRandomLink.get();
			} while (destinationLink.equals(originLink));

			double departureTime = sampleDepartureTime(timeDependent);

			Person person = population.getFactory().createPerson(Id.createPersonId("person_" + i));
			Plan plan = population.getFactory().createPlan();

			Activity home = population.getFactory().createActivityFromLinkId("home", originLink.getId());
			home.setEndTime(departureTime);
			plan.addActivity(home);

			Leg leg = population.getFactory().createLeg("drt");
			plan.addLeg(leg);

			Activity work = population.getFactory().createActivityFromLinkId("work", destinationLink.getId());
			plan.addActivity(work);

			person.addPlan(plan);
			population.addPerson(person);
		}
	}
}
