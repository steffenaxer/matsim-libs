package org.matsim.core.population.io;

import com.jerolba.carpet.CarpetParquetWriter;
import com.jerolba.carpet.io.OutputStreamOutputFile;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.population.*;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.population.routes.NetworkRoute;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordinateTransformation;
import org.matsim.core.utils.geometry.transformations.IdentityTransformation;
import org.matsim.core.utils.misc.Counter;
import org.matsim.core.utils.misc.Time;
import org.matsim.utils.objectattributes.ObjectAttributesConverter;
import org.matsim.utils.objectattributes.attributable.Attributes;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author steffenaxer
 */
public class PopulationWriterParquetV6 {
	private Counter counter = new Counter("[" + this.getClass().getSimpleName() + "] dumped person # ");
	private final Population population;
	private final ObjectAttributesConverter objectAttributesConverter = new ObjectAttributesConverter();
	private final CoordinateTransformation coordinateTransformation;
	public PopulationWriterParquetV6(Population population) {
		this(population,new IdentityTransformation());
	}
	public PopulationWriterParquetV6(Population population, CoordinateTransformation coordinateTransformation) {
		this.population = population;
		this.coordinateTransformation = coordinateTransformation;
	}

	record AttributesRecord(Map<String, String> attributes) {
	}

	record PersonRecord(String personId, List<PlanRecord> plans, AttributesRecord attributesRecord) {
	}

	record PlanRecord(Double score, List<PlanElementRecord> planElementRecords) {
	}

	record LegRecord(String mode, String routingMode, String departureTime, String travelTime, RouteRecord routeRecord,
							 AttributesRecord attributesRecord) {
	}

	record PlanElementRecord(ActivityRecord activityRecord, LegRecord legRecord) {
	}

	record RouteRecord(String routeType, String startLink, String endLink, String travelTime, double distance, String vehicleRefId,
							   String description) {
	}

	record ActivityRecord(String linkId, String facilityId, String type, String startTime, String endTime, String maxDuration,
								  String coords, AttributesRecord attributesRecord) {
	}

	public static void main(String[] args) throws IOException {
		String file = "/Users/steffenaxer/Downloads/kelheim-v3.0-1pct.output_plans.xml.gz";
		Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
		new PopulationReader(scenario).readFile(file);
		new PopulationWriterParquetV6(scenario.getPopulation()).write("/Users/steffenaxer/Downloads/test.parquet");
	}

	private static ParquetWriter<PersonRecord> getWriter(OutputStreamOutputFile outputStream) throws IOException {
		return CarpetParquetWriter.builder(outputStream, PersonRecord.class)
			.withWriteMode(Mode.OVERWRITE)
			.withCompressionCodec(CompressionCodecName.ZSTD)
			.build();
	}

	public ObjectAttributesConverter getObjectAttributesConverter() {
		return objectAttributesConverter;
	}

	public void write(final String filename) throws IOException {
		final OutputStreamOutputFile out = new OutputStreamOutputFile(new FileOutputStream(filename));
		try (ParquetWriter<PersonRecord> writer = getWriter(out)) {
			this.population.getPersons().values().forEach(
				p -> {
					try {
						writer.write(getPersonRecord(p, objectAttributesConverter, coordinateTransformation));
						counter.incCounter();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
		}
	}

	public static PersonRecord getPersonRecord(Person person, ObjectAttributesConverter objectAttributesConverter, CoordinateTransformation coordinateTransformation) {
		List<PlanRecord> planRecords = new ArrayList<>();
		person.getPlans().forEach(plan -> {
			List<PlanElementRecord> planElementRecords = new ArrayList<>();
			plan.getPlanElements().forEach(pe -> {
				if (pe instanceof Leg leg) {
					planElementRecords.add(getLegRecord(leg, objectAttributesConverter));
				} else if (pe instanceof Activity activity) {
					planElementRecords.add(createActivityRecord(activity, objectAttributesConverter, coordinateTransformation));
				}
			});
			Double score = plan.getScore() == null ? null : plan.getScore();
			PlanRecord planRecord = new PlanRecord(score, planElementRecords);
			planRecords.add(planRecord);
		});

		return new PersonRecord(person.getId().toString(), planRecords, createAttributesRecord(person.getAttributes(), objectAttributesConverter));
	}

	private static PlanElementRecord getLegRecord(Leg leg, ObjectAttributesConverter objectAttributesConverter) {
		RouteRecord routeRecord = getRouteRecord(leg.getRoute());
		return new PlanElementRecord(null, new LegRecord(leg.getMode(),
			leg.getRoutingMode(),
			Time.writeTime(leg.getDepartureTime()),
			Time.writeTime(leg.getTravelTime()),
			routeRecord,
			createAttributesRecord(leg.getAttributes(), objectAttributesConverter)));
	}

	private static RouteRecord getRouteRecord(Route route) {
		if (route == null) {
			return null;
		}

		String vehicleRefId = null;
		if (route instanceof NetworkRoute networkRoute && networkRoute.getVehicleId() != null) {
			vehicleRefId = networkRoute.getVehicleId().toString();
		}
		return new RouteRecord(route.getRouteType(),
			route.getStartLinkId().toString(),
			route.getEndLinkId().toString(),
			Time.writeTime(route.getTravelTime()),
			route.getDistance(),
			vehicleRefId,
			route.getRouteDescription());
	}

	private static PlanElementRecord createActivityRecord(Activity activity, ObjectAttributesConverter objectAttributesConverter, CoordinateTransformation coordinateTransformation) {
		final Coord coord = coordinateTransformation.transform( activity.getCoord() );
		return new PlanElementRecord(new ActivityRecord(
			activity.getLinkId() == null ? null : activity.getLinkId().toString(),
			activity.getFacilityId() == null ? null : activity.getFacilityId().toString(),
			activity.getType(),
			Time.writeTime(activity.getStartTime()),
			Time.writeTime(activity.getEndTime()),
			Time.writeTime(activity.getMaximumDuration()),
			coord == null ? null : coord.toString(),
			createAttributesRecord(activity.getAttributes(),objectAttributesConverter)), null);
	}

	private static AttributesRecord createAttributesRecord(Attributes attributes, ObjectAttributesConverter objectAttributesConverter) {
		return new AttributesRecord(attributes.getAsMap().entrySet().stream()
			.collect(Collectors.toMap(Map.Entry::getKey, e -> objectAttributesConverter.convertToString(e.getValue()))));
	}

}
