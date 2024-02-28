package org.matsim.core.events.algorithms;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.ActivityEndEvent;
import org.matsim.api.core.v01.events.ActivityStartEvent;
import org.matsim.api.core.v01.events.Event;
import org.matsim.api.core.v01.events.GenericEvent;
import org.matsim.api.core.v01.events.HasFacilityId;
import org.matsim.api.core.v01.events.HasLinkId;
import org.matsim.api.core.v01.events.HasPersonId;
import org.matsim.api.core.v01.events.LinkEnterEvent;
import org.matsim.api.core.v01.events.LinkLeaveEvent;
import org.matsim.api.core.v01.events.PersonArrivalEvent;
import org.matsim.api.core.v01.events.PersonDepartureEvent;
import org.matsim.api.core.v01.events.PersonEntersVehicleEvent;
import org.matsim.api.core.v01.events.PersonLeavesVehicleEvent;
import org.matsim.api.core.v01.events.PersonMoneyEvent;
import org.matsim.api.core.v01.events.PersonScoreEvent;
import org.matsim.api.core.v01.events.PersonStuckEvent;
import org.matsim.api.core.v01.events.TransitDriverStartsEvent;
import org.matsim.api.core.v01.events.VehicleAbortsEvent;
import org.matsim.api.core.v01.events.VehicleEntersTrafficEvent;
import org.matsim.api.core.v01.events.VehicleLeavesTrafficEvent;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.api.experimental.events.AgentWaitingForPtEvent;
import org.matsim.core.api.experimental.events.BoardingDeniedEvent;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.api.experimental.events.TeleportationArrivalEvent;
import org.matsim.core.api.experimental.events.VehicleArrivesAtFacilityEvent;
import org.matsim.core.api.experimental.events.VehicleDepartsAtFacilityEvent;
import org.matsim.core.api.internal.MatsimReader;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.MatsimEventsReader;
import org.matsim.core.events.MatsimEventsReader.CustomEventMapper;
import org.matsim.core.events.algorithms.EventWriterParquet.EventRecord;
import org.matsim.facilities.ActivityFacility;
import org.matsim.pt.transitSchedule.api.Departure;
import org.matsim.pt.transitSchedule.api.TransitLine;
import org.matsim.pt.transitSchedule.api.TransitRoute;
import org.matsim.pt.transitSchedule.api.TransitStopFacility;
import org.matsim.vehicles.Vehicle;

import com.jerolba.carpet.CarpetReader;

public class EventReaderParquet implements MatsimReader {
    private CarpetReader<EventRecord> reader;
    private final EventsManager events;
    private final Map<String, CustomEventMapper> customEventMappers = new HashMap<>();

    public EventReaderParquet(final EventsManager events) {
        this.events = events;
    }

    public static void main(String[] args) {
        EventsManager eventsManager = EventsUtils.createEventsManager();
        EventReaderParquet eventsReader = new EventReaderParquet(eventsManager);
        eventsManager.initProcessing();
        eventsReader.readFile("C:\\dev\\tmp\\events3.parquet");
        eventsManager.finishProcessing();
    }

    private CarpetReader<EventRecord> getReader(File filename) {
        return new CarpetReader<>(filename, EventRecord.class);
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

    @Override
    public void readFile(String filename) {
            this.reader = getReader(new File(filename));
            this.read();
    }

    private void read() {
        Iterator<EventRecord> iterator = reader.iterator();
        while (iterator.hasNext()) {
            EventRecord r = iterator.next();
            processEvent(r);
        }
    }

    static class AttributeAdapter {
        final EventRecord eventRecord;
        final Map<String, String> attributes;

        AttributeAdapter(EventRecord eventRecord) {
            this.eventRecord = eventRecord;
            this.attributes = eventRecord.attributes();
            this.addBasicData();
        }

        private void addBasicData() {
            this.attributes.put(("time"), String.valueOf(eventRecord.time()));
            this.attributes.put("type", eventRecord.tpye());
        }

        public String getValue(String key) {
            return this.attributes.get(key);
        }

        public int getLength() {
            return attributes.size();
        }

        public Map<String, String> getMap() {
            return attributes;
        }
    }

    private void processEvent(EventRecord eventRecord) {
        startEvent(new AttributeAdapter(eventRecord));
    }

    // This is a copy from EventsReaderXMLv1. Could should be centralized.
    private void startEvent(AttributeAdapter atts) {
        double time = Double.parseDouble(atts.getValue("time"));
        String eventType = atts.getValue("type");

        // === material related to wait2link below here ===
        if (LinkLeaveEvent.EVENT_TYPE.equals(eventType)) {
            this.events.processEvent(new LinkLeaveEvent(time,
                    Id.create(atts.getValue(LinkLeaveEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
                    Id.create(atts.getValue(LinkLeaveEvent.ATTRIBUTE_LINK), Link.class)
            // had driver id in previous version
            ));
        } else if (LinkEnterEvent.EVENT_TYPE.equals(eventType)) {
            this.events.processEvent(new LinkEnterEvent(time,
                    Id.create(atts.getValue(LinkEnterEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
                    Id.create(atts.getValue(LinkEnterEvent.ATTRIBUTE_LINK), Link.class)
            // had driver id in previous version
            ));
        } else if (VehicleEntersTrafficEvent.EVENT_TYPE.equals(eventType)) {
            // (this is the new version, marked by the new events name)

            this.events.processEvent(new VehicleEntersTrafficEvent(time,
                    Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
                    Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_LINK), Link.class),
                    Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
                    atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_NETWORKMODE),
                    Double.parseDouble(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION))));
        } else if ("wait2link".equals(eventType)) {
            // (this is the old version, marked by the old events name)

            // retrofit vehicle Id:
            Id<Vehicle> vehicleId;
            if (atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE) != null) {
                vehicleId = Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class);
            } else {
                // for the old events type, we set the vehicle id to the driver id if the
                // vehicle id does not exist:
                vehicleId = Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Vehicle.class);
            }
            // retrofit position:
            double position;
            if (atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION) != null) {
                position = Double.parseDouble(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION));
            } else {
                position = 1.0;
            }
            this.events.processEvent(new VehicleEntersTrafficEvent(time,
                    Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
                    Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_LINK), Link.class),
                    vehicleId,
                    atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_NETWORKMODE),
                    position));
        } else if (VehicleLeavesTrafficEvent.EVENT_TYPE.equals(eventType)) {
            this.events.processEvent(new VehicleLeavesTrafficEvent(time,
                    Id.create(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_DRIVER), Person.class),
                    Id.create(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_LINK), Link.class),
                    atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_VEHICLE) == null ? null
                            : Id.create(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
                    atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_NETWORKMODE),
                    Double.parseDouble(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_POSITION))));
        }
        // === material related to wait2link above here
        else if (ActivityEndEvent.EVENT_TYPE.equals(eventType)) {
            Coord coord = null;
            if (atts.getValue(Event.ATTRIBUTE_X) != null) {
                double xx = Double.parseDouble(atts.getValue(Event.ATTRIBUTE_X));
                double yy = Double.parseDouble(atts.getValue(Event.ATTRIBUTE_Y));
                coord = new Coord(xx, yy);
            }
            this.events.processEvent(new ActivityEndEvent(
                    time,
                    Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
                    Id.create(atts.getValue(HasLinkId.ATTRIBUTE_LINK), Link.class),
                    atts.getValue(HasFacilityId.ATTRIBUTE_FACILITY) == null ? null
                            : Id.create(atts.getValue(HasFacilityId.ATTRIBUTE_FACILITY),
                                    ActivityFacility.class),
                    atts.getValue(ActivityEndEvent.ATTRIBUTE_ACTTYPE),
                    coord));
        } else if (ActivityStartEvent.EVENT_TYPE.equals(eventType)) {
            Coord coord = null;
            if (atts.getValue(Event.ATTRIBUTE_X) != null) {
                double xx = Double.parseDouble(atts.getValue(Event.ATTRIBUTE_X));
                double yy = Double.parseDouble(atts.getValue(Event.ATTRIBUTE_Y));
                coord = new Coord(xx, yy);
            }
            this.events.processEvent(new ActivityStartEvent(
                    time,
                    Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
                    Id.create(atts.getValue(HasLinkId.ATTRIBUTE_LINK), Link.class),
                    atts.getValue(HasFacilityId.ATTRIBUTE_FACILITY) == null ? null
                            : Id.create(atts.getValue(
                                    HasFacilityId.ATTRIBUTE_FACILITY), ActivityFacility.class),
                    atts.getValue(ActivityStartEvent.ATTRIBUTE_ACTTYPE),
                    coord));
        } else if (PersonArrivalEvent.EVENT_TYPE.equals(eventType)) {
            String legMode = atts.getValue(PersonArrivalEvent.ATTRIBUTE_LEGMODE);
            String mode = legMode == null ? null : legMode.intern();
            this.events.processEvent(new PersonArrivalEvent(time,
                    Id.create(atts.getValue(PersonArrivalEvent.ATTRIBUTE_PERSON), Person.class),
                    Id.create(atts.getValue(PersonArrivalEvent.ATTRIBUTE_LINK), Link.class), mode));
        } else if (PersonDepartureEvent.EVENT_TYPE.equals(eventType)) {
            String legMode = atts.getValue(PersonDepartureEvent.ATTRIBUTE_LEGMODE);
            String canonicalLegMode = legMode == null ? null : legMode.intern();
            String routingMode = atts.getValue(PersonDepartureEvent.ATTRIBUTE_ROUTING_MODE);
            String canonicalRoutingMode = routingMode == null ? null : routingMode.intern();
            this.events.processEvent(new PersonDepartureEvent(time,
                    Id.create(atts.getValue(PersonDepartureEvent.ATTRIBUTE_PERSON), Person.class),
                    Id.create(atts.getValue(PersonDepartureEvent.ATTRIBUTE_LINK), Link.class), canonicalLegMode,
                    canonicalRoutingMode));
        } else if (PersonStuckEvent.EVENT_TYPE.equals(eventType)) {
            String legMode = atts.getValue(PersonStuckEvent.ATTRIBUTE_LEGMODE);
            String mode = legMode == null ? null : legMode.intern();
            String linkIdString = atts.getValue(PersonStuckEvent.ATTRIBUTE_LINK);
            Id<Link> linkId = linkIdString == null ? null : Id.create(linkIdString, Link.class); // linkId is optional
            this.events.processEvent(new PersonStuckEvent(time,
                    Id.create(atts.getValue(PersonStuckEvent.ATTRIBUTE_PERSON), Person.class), linkId, mode));
        } else if (VehicleAbortsEvent.EVENT_TYPE.equals(eventType)) {
            String linkIdString = atts.getValue(VehicleAbortsEvent.ATTRIBUTE_LINK);
            Id<Link> linkId = linkIdString == null ? null : Id.create(linkIdString, Link.class);
            this.events.processEvent(new VehicleAbortsEvent(time,
                    Id.create(atts.getValue(VehicleAbortsEvent.ATTRIBUTE_VEHICLE), Vehicle.class), linkId));
        } else if (PersonMoneyEvent.EVENT_TYPE.equals(eventType) || "agentMoney".equals(eventType)) {
            this.events.processEvent(new PersonMoneyEvent(time,
                    Id.create(atts.getValue(PersonMoneyEvent.ATTRIBUTE_PERSON), Person.class),
                    Double.parseDouble(atts.getValue(PersonMoneyEvent.ATTRIBUTE_AMOUNT)),
                    atts.getValue(PersonMoneyEvent.ATTRIBUTE_PURPOSE),
                    atts.getValue(PersonMoneyEvent.ATTRIBUTE_TRANSACTION_PARTNER)));
        } else if (PersonScoreEvent.EVENT_TYPE.equals(eventType) || "personScore".equals(eventType)) {
            this.events.processEvent(new PersonScoreEvent(time,
                    Id.create(atts.getValue(PersonScoreEvent.ATTRIBUTE_PERSON), Person.class),
                    Double.parseDouble(atts.getValue(PersonScoreEvent.ATTRIBUTE_AMOUNT)),
                    atts.getValue(PersonScoreEvent.ATTRIBUTE_KIND)));
        } else if (PersonEntersVehicleEvent.EVENT_TYPE.equals(eventType)) {
            String personString = atts.getValue(PersonEntersVehicleEvent.ATTRIBUTE_PERSON);
            String vehicleString = atts.getValue(PersonEntersVehicleEvent.ATTRIBUTE_VEHICLE);
            this.events.processEvent(new PersonEntersVehicleEvent(time, Id.create(personString, Person.class),
                    Id.create(vehicleString, Vehicle.class)));
        } else if (PersonLeavesVehicleEvent.EVENT_TYPE.equals(eventType)) {
            Id<Person> pId = Id.create(atts.getValue(PersonLeavesVehicleEvent.ATTRIBUTE_PERSON), Person.class);
            Id<Vehicle> vId = Id.create(atts.getValue(PersonLeavesVehicleEvent.ATTRIBUTE_VEHICLE), Vehicle.class);
            this.events.processEvent(new PersonLeavesVehicleEvent(time, pId, vId));
        } else if (TeleportationArrivalEvent.EVENT_TYPE.equals(eventType)) {
            this.events.processEvent(new TeleportationArrivalEvent(
                    time,
                    Id.create(atts.getValue(TeleportationArrivalEvent.ATTRIBUTE_PERSON), Person.class),
                    Double.parseDouble(atts.getValue(TeleportationArrivalEvent.ATTRIBUTE_DISTANCE)),
                    atts.getValue(TeleportationArrivalEvent.ATTRIBUTE_MODE)));
        } else if (VehicleArrivesAtFacilityEvent.EVENT_TYPE.equals(eventType)) {
            String delay = atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_DELAY);
            this.events.processEvent(new VehicleArrivesAtFacilityEvent(time,
                    Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
                    Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_FACILITY),
                            TransitStopFacility.class),
                    delay == null ? 0.0 : Double.parseDouble(delay)));
        } else if (VehicleDepartsAtFacilityEvent.EVENT_TYPE.equals(eventType)) {
            String delay = atts.getValue(VehicleDepartsAtFacilityEvent.ATTRIBUTE_DELAY);
            this.events.processEvent(new VehicleDepartsAtFacilityEvent(time,
                    Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
                    Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_FACILITY),
                            TransitStopFacility.class),
                    delay == null ? 0.0 : Double.parseDouble(delay)));
        } else if (TransitDriverStartsEvent.EVENT_TYPE.equals(eventType)) {
            this.events.processEvent(new TransitDriverStartsEvent(time,
                    Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_DRIVER_ID), Person.class),
                    Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_VEHICLE_ID), Vehicle.class),
                    Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_TRANSIT_LINE_ID), TransitLine.class),
                    Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_TRANSIT_ROUTE_ID), TransitRoute.class),
                    Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_DEPARTURE_ID), Departure.class)));
        } else if (BoardingDeniedEvent.EVENT_TYPE.equals(eventType)) {
            Id<Person> personId = Id.create(atts.getValue(BoardingDeniedEvent.ATTRIBUTE_PERSON_ID), Person.class);
            Id<Vehicle> vehicleId = Id.create(atts.getValue(BoardingDeniedEvent.ATTRIBUTE_VEHICLE_ID), Vehicle.class);
            this.events.processEvent(new BoardingDeniedEvent(time, personId, vehicleId));
        } else if (AgentWaitingForPtEvent.EVENT_TYPE.equals(eventType)) {
            Id<Person> agentId = Id.create(atts.getValue(AgentWaitingForPtEvent.ATTRIBUTE_AGENT), Person.class);
            Id<TransitStopFacility> waitStopId = Id.create(atts.getValue(AgentWaitingForPtEvent.ATTRIBUTE_WAITSTOP),
                    TransitStopFacility.class);
            Id<TransitStopFacility> destinationStopId = Id
                    .create(atts.getValue(AgentWaitingForPtEvent.ATTRIBUTE_DESTINATIONSTOP), TransitStopFacility.class);
            this.events.processEvent(new AgentWaitingForPtEvent(time, agentId, waitStopId, destinationStopId));
        } else {
            GenericEvent event = new GenericEvent(eventType, time);
            atts.getMap().entrySet().stream().filter(e -> !(e.getKey().equals("time") || e.getKey().equals("type")))
                    .forEach(e -> {
                        String key = e.getKey();
                        String value = e.getKey();
                        event.getAttributes().put(key, value);
                    });
            MatsimEventsReader.CustomEventMapper cem = customEventMappers.get(eventType);
            if (cem != null) {
                this.events.processEvent(cem.apply(event));
            } else {
                this.events.processEvent(event);
            }
        }
    }

    public void addCustomEventMapper(String eventType, CustomEventMapper cem) {
        this.customEventMappers.put(eventType, cem);
    }

}
