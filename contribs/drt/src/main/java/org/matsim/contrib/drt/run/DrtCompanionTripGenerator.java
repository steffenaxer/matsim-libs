package org.matsim.contrib.drt.run;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.Activity;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.Plan;
import org.matsim.api.core.v01.population.Population;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.controler.events.IterationEndsEvent;
import org.matsim.core.controler.events.IterationStartsEvent;
import org.matsim.core.controler.listener.IterationEndsListener;
import org.matsim.core.controler.listener.IterationStartsListener;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.population.PopulationUtils;
import org.matsim.core.router.MainModeIdentifier;
import org.matsim.core.router.TripRouter;
import org.matsim.core.router.TripStructureUtils;
import org.matsim.facilities.ActivityFacilities;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.*;

/**
 * @author steffenaxer
 */
public final class DrtCompanionTripGenerator implements IterationStartsListener, IterationEndsListener {

	private static final Logger LOG = Logger.getLogger(DrtCompanionTripGenerator.class);
	private final Config config;
	private final Scenario scenario;
	private final Network network;
	private final Population population;
	private final ActivityFacilities activityFacilities;
	private final Provider<TripRouter> tripRouterProvider;
	private Set<String> drtModes = new HashSet<String>();
	public final static String PSEUDO_DRT_AGENT_NAME = "COMPANION";
	private final MainModeIdentifier mainModeIdentifier;
	private double companionProbability = 0.25;
	private final Random rng = MatsimRandom.getLocalInstance();

	@Inject
	DrtCompanionTripGenerator(MainModeIdentifier mainModeIdentifier, Config config, Scenario scenario, Network network,
							  Population population, ActivityFacilities activityFacilities, Provider<TripRouter> tripRouterProvider) {
		this.config = config;
		this.scenario = scenario;
		this.network = network;
		this.population = population;
		this.activityFacilities = activityFacilities;
		this.tripRouterProvider = tripRouterProvider;
		this.mainModeIdentifier = mainModeIdentifier;
		getDrtModes(config);
	}

	private void getDrtModes(Config config) {
		if (config.getModules().containsKey(MultiModeDrtConfigGroup.GROUP_NAME)) {
			MultiModeDrtConfigGroup multiModeDrtConfigGroup = ConfigUtils.addOrGetModule(config, MultiModeDrtConfigGroup.class);
			for (DrtConfigGroup drtConfigGroup : multiModeDrtConfigGroup.getModalElements()) {
				this.drtModes.add(drtConfigGroup.getMode());
			}
		}
	}

	private String getCompanionName(String drtMode) {
		return PSEUDO_DRT_AGENT_NAME + "_" + drtMode;
	}

	void addCompanionAgents() {
		HashMap<String, Integer> drtCompanionAgents = new HashMap<String, Integer>();
		Collection<Person> companions = new ArrayList<Person>();
		for (Person person : this.scenario.getPopulation().getPersons().values()) {
			for (Plan selectedPlan : person.getPlans()) {
				for (TripStructureUtils.Trip trip : TripStructureUtils.getTrips(selectedPlan)) {
					String mainMode = mainModeIdentifier.identifyMainMode(trip.getTripElements());
					if (this.drtModes.contains(mainMode)) {
						if(rng.nextDouble()<this.companionProbability)
						{
							int currentCounter = drtCompanionAgents.getOrDefault(mainMode, 0);
							currentCounter++;
							drtCompanionAgents.put(mainMode, currentCounter);
							companions.add(createCompanionAgent(mainMode, person.getId(), trip, trip.getOriginActivity(), trip.getDestinationActivity()));
						}
					}
				}
			}
		}
		companions.stream().forEach(p -> {
			this.scenario.getPopulation().addPerson(p);
		});


		for (Map.Entry<String, Integer> drtModeEntry : drtCompanionAgents.entrySet()) {
			LOG.info("Added # " + drtModeEntry.getValue() + " drt companion agents for mode " + drtModeEntry.getKey());
		}
	}

	private Person createCompanionAgent(String drtMode, Id<Person> originalPersonId, TripStructureUtils.Trip trip, Activity fromActivity, Activity toActivity) {
		String prefix = getCompanionName(drtMode);
		String companionId = prefix + "_" + originalPersonId.toString() + "_" + UUID.randomUUID();
		Person person = PopulationUtils.getFactory().createPerson(Id.createPersonId(companionId));
		Plan plan = PopulationUtils.createPlan();
		plan.getPlanElements().add(fromActivity);
		plan.getPlanElements().addAll(trip.getTripElements());
		plan.getPlanElements().add(toActivity);
		person.addPlan(plan);
		return person;
	}

	void removeCompanionAgents() {
		int counter = 0;
		Iterator<? extends Person> it = this.scenario.getPopulation().getPersons().values().iterator();
		while (it.hasNext()) {
			Person person = it.next();
			if (person.getId().toString().startsWith(PSEUDO_DRT_AGENT_NAME)) {
				it.remove();
				counter++;
			}
		}
		LOG.info("Removed # " + counter + " drt companion agents");
	}

	@Override
	public void notifyIterationEnds(IterationEndsEvent event) {
		this.removeCompanionAgents();
	}

	@Override
	public void notifyIterationStarts(IterationStartsEvent event) {
		this.addCompanionAgents();
	}
}
