import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.Plan;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.population.PopulationUtils;
import org.matsim.core.population.algorithms.PersonAlgorithm;
import org.matsim.core.population.io.PopulationReader;
import org.matsim.core.population.io.StreamingPopulationReader;
import org.matsim.core.population.io.StreamingPopulationWriter;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.utils.objectattributes.attributable.AttributesUtils;

/**
 * @author steffenaxer
 */
public class TestSenozon {
	public static void main(String[] args) {
		String inPop = "/Users/steffenaxer/Downloads/hamburg-v3.0-10pct-base.output_plans.10pct.xml.gz";
		String outPop = "/Users/steffenaxer/Downloads/output.xml.gz";
		createPopulationCopy(inPop, outPop);

		// Compare plans on xml scope
		comparePopulations(inPop, outPop);
	}

	static void createPopulationCopy(String inPop, String outPop) {
		//Prepare Streaming Writer
		StreamingPopulationWriter w = new StreamingPopulationWriter();
		w.startStreaming(outPop);
		Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());

		//Prepare Streaming Reader
		StreamingPopulationReader r = new StreamingPopulationReader(scenario);
		r.addAlgorithm(getPersonDuplicator(w));
		r.readFile(inPop);

		//Close Streaming Writer
		w.closeStreaming();
	}

	static PersonAlgorithm getPersonDuplicator(StreamingPopulationWriter w) {
		return new PersonAlgorithm() {
			@Override
			public void run(Person person) {
				Person clonedPerson = copyPerson(person);
				w.writePerson(clonedPerson);
			}
		};
	}

	static Person copyPerson(Person originalPerson) {
		Person clonedPerson = PopulationUtils.getFactory().createPerson(Id.createPersonId(originalPerson.getId()));

		// Copy person attributes
		AttributesUtils.copyTo(originalPerson.getAttributes(), clonedPerson.getAttributes());

		for (int i = 0; i<originalPerson.getPlans().size(); i++)
		{
			// Copy plans
			Plan originalPlan = originalPerson.getPlans().get(i);
			Plan clonedPlan = PopulationUtils.createPlan(clonedPerson);
			PopulationUtils.copyFromTo(originalPlan, clonedPlan);
			clonedPerson.addPlan(clonedPlan);

			// Compare cloned Plans on object scope
			comparePlans(originalPlan, clonedPlan);
		}

		return clonedPerson;
	}

	public static void comparePopulations(String a, String b) {
		Scenario scenarioReference = ScenarioUtils.createScenario(ConfigUtils.createConfig());
		Scenario scenarioCurrent = ScenarioUtils.createScenario(ConfigUtils.createConfig());
		new PopulationReader(scenarioReference).readFile(a);
		new PopulationReader(scenarioCurrent).readFile(b);

		for (Person originalPerson : scenarioReference.getPopulation().getPersons().values()) {
			Person clonedPerson = scenarioCurrent.getPopulation().getPersons().get(originalPerson.getId());
			for (int i = 0; i<originalPerson.getPlans().size(); i++)
			{
				comparePlans(originalPerson.getPlans().get(i), clonedPerson.getPlans().get(i));
			}
		}
	}

	private static void comparePlans(Plan original, Plan copy) {
		String a = original.toString();
		String b = copy.toString();
		if (!a.equals(b)) {
			throw new IllegalStateException("Plans are not the same");
		}

		for (int i = 0; i < original.getPlanElements().size(); i++) {
			String aPle = original.getPlanElements().get(i).toString();
			String bPle = copy.getPlanElements().get(i).toString();

			if (!aPle.equals(bPle)) {
				throw new IllegalStateException("PlanElements are not the same");
			}
		}
	}
}
