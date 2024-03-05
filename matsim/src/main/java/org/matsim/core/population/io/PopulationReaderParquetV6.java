package org.matsim.core.population.io;

import com.jerolba.carpet.CarpetReader;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.population.*;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.api.internal.MatsimReader;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.algorithms.EventReaderParquet;
import org.matsim.core.events.algorithms.EventWriterParquet;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;

import org.matsim.core.population.PersonUtils;
import org.matsim.core.population.PopulationUtils;
import org.matsim.core.population.io.PopulationWriterParquetV6.*;
import org.matsim.core.router.TripStructureUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.misc.Time;

/**
 * @author steffenaxer
 */
public class PopulationReaderParquetV6 implements MatsimReader {

	Population population;
	private CarpetReader<PersonRecord> reader;

	PopulationReaderParquetV6(Population population)
	{
		this.population=population;
	}

	public static void main(String[] args) {
		Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
		PopulationReaderParquetV6 reader = new PopulationReaderParquetV6(scenario.getPopulation());
		reader.readFile("/Users/steffenaxer/Downloads/test.parquet");
	}

	private CarpetReader<PersonRecord> getReader(File filename) {
		return new CarpetReader<>(filename, PersonRecord.class);
	}

	@Override
	public void readFile(String filename) {
		this.reader = getReader(new File(filename));
		this.read();
	}

	@Override
	public void readURL(URL url) {
		try {
			this.reader = getReader(new File(url.toURI()));
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
		this.read();
	}

	private void read() {
		Iterator<PersonRecord> iterator = reader.iterator();
		while (iterator.hasNext()) {
			PersonRecord personRecord = iterator.next();
			getPerson(personRecord);
		}
	}

	private static Person getPerson(PersonRecord personRecord)
	{
		return null;
	}

	private static Plan getPlan(PlanRecord planRecord)
	{
		Plan plan = PopulationUtils.createPlan();
		planRecord.planElementRecords().forEach(pe ->
		{
			if(pe.activityRecord()!=null)
			{

			} else  if (pe.legRecord()!=null)
			{

			}
		});

		return null;

	}

	private static Leg getLeg(LegRecord legRecord)
	{
		Leg leg = PopulationUtils.createLeg(legRecord.mode());
		leg.setTravelTime(Time.parseTime(legRecord.travelTime()));
		leg.setDepartureTime(Time.parseTime(legRecord.departureTime()));
		leg.setRoutingMode(leg.getRoutingMode());
		leg.setRoute(null);
		return leg;
	}

	private static Activity getActivity(ActivityRecord activityRecord)
	{
		Activity activity;
		if(TripStructureUtils.isStageActivityType(activityRecord.type()))
		{
			activity = PopulationUtils.createInteractionActivityFromFacilityId(activityRecord.type(), );
		}
		return null;
	}

	private static Route getRoute(RouteRecord routeRecord)
	{
		routeRecord.
		return null;
	}








}
