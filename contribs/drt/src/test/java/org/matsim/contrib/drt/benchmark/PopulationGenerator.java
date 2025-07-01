package org.matsim.contrib.drt.benchmark;

import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.population.*;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.Id;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.population.io.PopulationWriter;
import org.matsim.api.core.v01.network.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PopulationGenerator {

    private final static Random random = MatsimRandom.getLocalInstance();

    public static void generatePopulation(int numberOfAgents,Scenario scenario) {
        Population population = scenario.getPopulation();
        List<Node> nodes = new ArrayList<>(scenario.getNetwork().getNodes().values());

        for (int i = 0; i < numberOfAgents; i++) {
            // Random origin and destination
            Node origin = nodes.get(random.nextInt(nodes.size()));
            Node destination;
            do {
                destination = nodes.get(random.nextInt(nodes.size()));
            } while (destination.equals(origin));

            // Random departure time (0 - 86400 seconds)
            double departureTime = random.nextDouble() * 86400;

            // Create person and plan
            Person person = population.getFactory().createPerson(Id.createPersonId("person_" + i));
            Plan plan = population.getFactory().createPlan();

            // Home activity at origin
            Activity home = population.getFactory().createActivityFromCoord("home", origin.getCoord());
            home.setEndTime(departureTime);
            plan.addActivity(home);

            // DRT leg
            Leg leg = population.getFactory().createLeg("drt");
            plan.addLeg(leg);

            // Work activity at destination
            Activity work = population.getFactory().createActivityFromCoord("work", destination.getCoord());
            plan.addActivity(work);

            person.addPlan(plan);
            population.addPerson(person);
        }
    }
}
