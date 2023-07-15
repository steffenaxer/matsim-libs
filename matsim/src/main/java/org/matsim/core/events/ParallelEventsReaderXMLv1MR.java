package org.matsim.core.events;

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
import org.matsim.facilities.ActivityFacility;
import org.matsim.pt.transitSchedule.api.Departure;
import org.matsim.pt.transitSchedule.api.TransitLine;
import org.matsim.pt.transitSchedule.api.TransitRoute;
import org.matsim.pt.transitSchedule.api.TransitStopFacility;
import org.matsim.vehicles.Vehicle;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.AttributesImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author steffenaxer, mrieser
 */
public class ParallelEventsReaderXMLv1MR extends MatsimXmlEventsParser {
	static public final String EVENT = "event";
	static public final String EVENTS = "events";
	private static final int THREADS_LIMIT = 4;
	public final static String CLOSING_MARKER = UUID.randomUUID().toString();
	private final Map<String, MatsimEventsReader.CustomEventMapper> customEventMappers = new HashMap<>();
	final BlockingQueue<EventData> eventDataQueue = new LinkedBlockingQueue<>(10_000);
	final BlockingQueue<CompletableFuture<Event>> futureEventsQueue = new LinkedBlockingQueue<>();
	final EventsManager eventsManager;
	Thread[] workerThreads;
	Thread inserterThread;

	public ParallelEventsReaderXMLv1MR(EventsManager eventsManager) {
		this.eventsManager = eventsManager;
		this.initializeWorkerThreads();
		this.initializeInserterThread();
	}

	void initializeWorkerThreads() {
		workerThreads = new Thread[THREADS_LIMIT];
		for (int i = 0; i < THREADS_LIMIT; i++) {
			MatsimEventsWorker runner =
					new MatsimEventsWorker(this.eventDataQueue, this.customEventMappers);
			Thread thread = new Thread(runner);
			thread.setDaemon(true);
			thread.setName(MatsimEventsWorker.class.toString() + i);
			workerThreads[i] = thread;
			thread.start();
		}
	}

	void initializeInserterThread() {
		MatsimEventsInserter eventsInserter = new MatsimEventsInserter(this.eventsManager, this.futureEventsQueue);
		Thread thread = new Thread(eventsInserter);
		thread.setDaemon(true);
		thread.setName(MatsimEventsInserter.class.toString());
		thread.start();
		this.inserterThread = thread;
	}

	@Override
	public void addCustomEventMapper(String eventType, MatsimEventsReader.CustomEventMapper cem) {
		customEventMappers.put(eventType, cem);
	}

	@Override
	public void startTag(String name, Attributes atts, Stack<String> context) {
		if (EVENT.equals(name)) {
			String eventType = atts.getValue("type");
			String time = atts.getValue("time");

			try {
				CompletableFuture<Event> futureEvent = new CompletableFuture<>();

				if (LinkLeaveEvent.EVENT_TYPE.equals(eventType)) {
					this.eventDataQueue.put(new EventData(futureEvent, eventType, time, atts, LinkLeaveEvent.ATTRIBUTE_VEHICLE, LinkLeaveEvent.ATTRIBUTE_LINK));
				} else if (LinkEnterEvent.EVENT_TYPE.equals(eventType)) {
					this.eventDataQueue.put(new EventData(futureEvent, eventType, time, atts, LinkEnterEvent.ATTRIBUTE_VEHICLE, LinkEnterEvent.ATTRIBUTE_LINK));
				} else if (ActivityEndEvent.EVENT_TYPE.equals(eventType)) {
					this.eventDataQueue.put(new EventData(futureEvent, eventType, time, atts, Event.ATTRIBUTE_X, Event.ATTRIBUTE_Y, HasPersonId.ATTRIBUTE_PERSON, HasLinkId.ATTRIBUTE_LINK, HasFacilityId.ATTRIBUTE_FACILITY, ActivityEndEvent.ATTRIBUTE_ACTTYPE));
				} else if (ActivityStartEvent.EVENT_TYPE.equals(eventType)) {
					this.eventDataQueue.put(new EventData(futureEvent, eventType, time, atts, Event.ATTRIBUTE_X, Event.ATTRIBUTE_Y, HasPersonId.ATTRIBUTE_PERSON, HasLinkId.ATTRIBUTE_LINK, HasFacilityId.ATTRIBUTE_FACILITY, ActivityStartEvent.ATTRIBUTE_ACTTYPE));
				} else {
					// fall back, make a (expensive) copy of all attributes
					this.eventDataQueue.put(new EventData(futureEvent, eventType, time, atts));
				}

				this.futureEventsQueue.put(futureEvent);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void endTag(String name, String content, Stack<String> context) {
		if (name.equals(EVENTS)) {
			this.submitClosingData();
			this.joinThreads();
		}
	}

	void submitClosingData() {
		CompletableFuture<Event> e = new CompletableFuture<>();
		e.complete(null);
		try {
			for (int i = 0; i < THREADS_LIMIT; i++) {
				EventData closingData = new EventData(e, CLOSING_MARKER, null, 0);
				this.eventDataQueue.put(closingData);
			}
			this.futureEventsQueue.put(e);
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}

	void joinThreads() {
		try {
			for (int i = 0; i < THREADS_LIMIT; i++) {
				this.workerThreads[i].join();
			}
			this.inserterThread.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static class EventData {
		final CompletableFuture<Event> futureEvent;
		final String type;
		final String time;
		final String[] attributes;
		final Attributes xmlAttributes;

		private EventData(CompletableFuture<Event> futureEvent, String type, String time, int attributesCount) {
			this.futureEvent = futureEvent;
			this.type = type;
			this.time = time;
			this.attributes = new String[attributesCount * 2];
			this.xmlAttributes = null;
		}

		public EventData(CompletableFuture<Event> futureEvent, String type, String time, Attributes xmlAttributes) {
			this.futureEvent = futureEvent;
			this.type = type;
			this.time = time;
			this.attributes = null;
			this.xmlAttributes = new AttributesImpl(xmlAttributes); // make a copy
		}

		public EventData(CompletableFuture<Event> futureEvent, String type, String time, Attributes xmlAttributes, String... neededAttributes) {
			this(futureEvent, type, time, neededAttributes.length);
			for (int i = 0; i < neededAttributes.length; i++) {
				String attributeName = neededAttributes[i];
				this.attributes[2 * i] = attributeName;
				this.attributes[2 * i + 1] = xmlAttributes.getValue(attributeName);
			}
		}

		public String getAttribute(String name) {
			if (this.attributes != null) {
				for (int i = 0; i < this.attributes.length; i += 2) {
					if (this.attributes[i].equals(name)) {
						return this.attributes[i + 1];
					}
				}
				return null;
			}
			// if we don't have only the needed attributes, search in all:
			return this.xmlAttributes.getValue(name);
		}
	}


	public final static class MatsimEventsInserter implements Runnable {
		final EventsManager eventsManager;
		final BlockingQueue<CompletableFuture<Event>> futureEventsQueue;


		MatsimEventsInserter(EventsManager eventsManager, BlockingQueue<CompletableFuture<Event>> futureEventsQueue) {
			this.eventsManager = eventsManager;
			this.futureEventsQueue = futureEventsQueue;
		}


		@Override
		public void run() {
			while (true) {
				try {
					Event event = futureEventsQueue.take().get();

					if (event == null) {
						return;
					}

					this.eventsManager.processEvent(event);
				} catch (InterruptedException | ExecutionException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	public final static class MatsimEventsWorker implements Runnable {
		private final BlockingQueue<EventData> eventsQueue;
		private final Map<String, MatsimEventsReader.CustomEventMapper> customEventMappers;

		MatsimEventsWorker(BlockingQueue<EventData> eventsQueue, Map<String, MatsimEventsReader.CustomEventMapper> customEventMappers) {
			this.eventsQueue = eventsQueue;
			this.customEventMappers = customEventMappers;
		}

		@Override
		public void run() {
			while (true) {
				try {
					EventData eventData = this.eventsQueue.take();
					//noinspection StringEquality
					if (eventData.type == CLOSING_MARKER) {
						return;
					}
					this.processEventData(eventData);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		void completeEvent(EventData eventData, Event event) {
			eventData.futureEvent.complete(event);
		}

		/**
		 * This code is mainly a copy from the EventsReaderXMLv1
		 */
		void processEventData(EventData eventData) {
			String eventType = eventData.type;
			double time = Double.parseDouble(eventData.time);

			// === material related to wait2link below here ===
			if (LinkLeaveEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new LinkLeaveEvent(time,
						Id.create(eventData.getAttribute(LinkLeaveEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						Id.create(eventData.getAttribute(LinkLeaveEvent.ATTRIBUTE_LINK), Link.class)
						// had driver id in previous version
				));
			} else if (LinkEnterEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new LinkEnterEvent(time,
						Id.create(eventData.getAttribute(LinkEnterEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						Id.create(eventData.getAttribute(LinkEnterEvent.ATTRIBUTE_LINK), Link.class)
						// had driver id in previous version
				));
			} else if (VehicleEntersTrafficEvent.EVENT_TYPE.equals(eventType)) {
				// (this is the new version, marked by the new events name)

				completeEvent(eventData, new VehicleEntersTrafficEvent(time,
						Id.create(eventData.getAttribute(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_LINK), Link.class),
						Id.create(eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_NETWORKMODE),
						Double.parseDouble(eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION))
				));
			} else if ("wait2link".equals(eventType)) {
				// (this is the old version, marked by the old events name)

				// retrofit vehicle Id:
				Id<Vehicle> vehicleId;
				if (eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE) != null) {
					vehicleId = Id.create(eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class);
				} else {
					// for the old events type, we set the vehicle id to the driver id if the vehicle id does not exist:
					vehicleId = Id.create(eventData.getAttribute(HasPersonId.ATTRIBUTE_PERSON), Vehicle.class);
				}
				// retrofit position:
				double position;
				if (eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION) != null) {
					position = Double.parseDouble(eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION));
				} else {
					position = 1.0;
				}
				completeEvent(eventData, new VehicleEntersTrafficEvent(time,
						Id.create(eventData.getAttribute(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_LINK), Link.class),
						vehicleId,
						eventData.getAttribute(VehicleEntersTrafficEvent.ATTRIBUTE_NETWORKMODE),
						position
				));
			} else if (VehicleLeavesTrafficEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new VehicleLeavesTrafficEvent(time,
						Id.create(eventData.getAttribute(VehicleLeavesTrafficEvent.ATTRIBUTE_DRIVER), Person.class),
						Id.create(eventData.getAttribute(VehicleLeavesTrafficEvent.ATTRIBUTE_LINK), Link.class),
						eventData.getAttribute(VehicleLeavesTrafficEvent.ATTRIBUTE_VEHICLE) == null ? null : Id.create(eventData.getAttribute(VehicleLeavesTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						eventData.getAttribute(VehicleLeavesTrafficEvent.ATTRIBUTE_NETWORKMODE),
						Double.parseDouble(eventData.getAttribute(VehicleLeavesTrafficEvent.ATTRIBUTE_POSITION))
				));
			}
			// === material related to wait2link above here
			else if (ActivityEndEvent.EVENT_TYPE.equals(eventType)) {
				Coord coord = null;
				if (eventData.getAttribute(Event.ATTRIBUTE_X) != null) {
					double xx = Double.parseDouble(eventData.getAttribute(Event.ATTRIBUTE_X));
					double yy = Double.parseDouble(eventData.getAttribute(Event.ATTRIBUTE_Y));
					coord = new Coord(xx, yy);
				}
				completeEvent(eventData, new ActivityEndEvent(
						time,
						Id.create(eventData.getAttribute(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(eventData.getAttribute(HasLinkId.ATTRIBUTE_LINK), Link.class),
						eventData.getAttribute(HasFacilityId.ATTRIBUTE_FACILITY) == null ? null : Id.create(eventData.getAttribute(HasFacilityId.ATTRIBUTE_FACILITY),
								ActivityFacility.class),
						eventData.getAttribute(ActivityEndEvent.ATTRIBUTE_ACTTYPE),
						coord));
			} else if (ActivityStartEvent.EVENT_TYPE.equals(eventType)) {
				Coord coord = null;
				if (eventData.getAttribute(Event.ATTRIBUTE_X) != null) {
					double xx = Double.parseDouble(eventData.getAttribute(Event.ATTRIBUTE_X));
					double yy = Double.parseDouble(eventData.getAttribute(Event.ATTRIBUTE_Y));
					coord = new Coord(xx, yy);
				}
				completeEvent(eventData, new ActivityStartEvent(
						time,
						Id.create(eventData.getAttribute(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(eventData.getAttribute(HasLinkId.ATTRIBUTE_LINK), Link.class),
						eventData.getAttribute(HasFacilityId.ATTRIBUTE_FACILITY) == null ? null : Id.create(eventData.getAttribute(
								HasFacilityId.ATTRIBUTE_FACILITY), ActivityFacility.class),
						eventData.getAttribute(ActivityStartEvent.ATTRIBUTE_ACTTYPE),
						coord));
			} else if (PersonArrivalEvent.EVENT_TYPE.equals(eventType)) {
				String legMode = eventData.getAttribute(PersonArrivalEvent.ATTRIBUTE_LEGMODE);
				String mode = legMode == null ? null : legMode.intern();
				completeEvent(eventData, new PersonArrivalEvent(time, Id.create(eventData.getAttribute(PersonArrivalEvent.ATTRIBUTE_PERSON), Person.class), Id.create(eventData.getAttribute(PersonArrivalEvent.ATTRIBUTE_LINK), Link.class), mode));
			} else if (PersonDepartureEvent.EVENT_TYPE.equals(eventType)) {
				String legMode = eventData.getAttribute(PersonDepartureEvent.ATTRIBUTE_LEGMODE);
				String canonicalLegMode = legMode == null ? null : legMode.intern();
				String routingMode = eventData.getAttribute(PersonDepartureEvent.ATTRIBUTE_ROUTING_MODE);
				String canonicalRoutingMode = routingMode == null ? null : routingMode.intern();
				completeEvent(eventData, new PersonDepartureEvent(time, Id.create(eventData.getAttribute(PersonDepartureEvent.ATTRIBUTE_PERSON), Person.class), Id.create(eventData.getAttribute(PersonDepartureEvent.ATTRIBUTE_LINK), Link.class), canonicalLegMode, canonicalRoutingMode));
			} else if (PersonStuckEvent.EVENT_TYPE.equals(eventType)) {
				String legMode = eventData.getAttribute(PersonStuckEvent.ATTRIBUTE_LEGMODE);
				String mode = legMode == null ? null : legMode.intern();
				String linkIdString = eventData.getAttribute(PersonStuckEvent.ATTRIBUTE_LINK);
				Id<Link> linkId = linkIdString == null ? null : Id.create(linkIdString, Link.class); // linkId is optional
				completeEvent(eventData, new PersonStuckEvent(time, Id.create(eventData.getAttribute(PersonStuckEvent.ATTRIBUTE_PERSON), Person.class), linkId, mode));
			} else if (VehicleAbortsEvent.EVENT_TYPE.equals(eventType)) {
				String linkIdString = eventData.getAttribute(VehicleAbortsEvent.ATTRIBUTE_LINK);
				Id<Link> linkId = linkIdString == null ? null : Id.create(linkIdString, Link.class);
				completeEvent(eventData, new VehicleAbortsEvent(time, Id.create(eventData.getAttribute(VehicleAbortsEvent.ATTRIBUTE_VEHICLE), Vehicle.class), linkId));
			} else if (PersonMoneyEvent.EVENT_TYPE.equals(eventType) || "agentMoney".equals(eventType)) {
				completeEvent(eventData, new PersonMoneyEvent(time, Id.create(eventData.getAttribute(PersonMoneyEvent.ATTRIBUTE_PERSON), Person.class), Double.parseDouble(eventData.getAttribute(PersonMoneyEvent.ATTRIBUTE_AMOUNT)), eventData.getAttribute(PersonMoneyEvent.ATTRIBUTE_PURPOSE), eventData.getAttribute(PersonMoneyEvent.ATTRIBUTE_TRANSACTION_PARTNER)));
			} else if (PersonScoreEvent.EVENT_TYPE.equals(eventType) || "personScore".equals(eventType)) {
				completeEvent(eventData, new PersonScoreEvent(time, Id.create(eventData.getAttribute(PersonScoreEvent.ATTRIBUTE_PERSON), Person.class), Double.parseDouble(eventData.getAttribute(PersonScoreEvent.ATTRIBUTE_AMOUNT)), eventData.getAttribute(PersonScoreEvent.ATTRIBUTE_KIND)));
			} else if (PersonEntersVehicleEvent.EVENT_TYPE.equals(eventType)) {
				String personString = eventData.getAttribute(PersonEntersVehicleEvent.ATTRIBUTE_PERSON);
				String vehicleString = eventData.getAttribute(PersonEntersVehicleEvent.ATTRIBUTE_VEHICLE);
				completeEvent(eventData, new PersonEntersVehicleEvent(time, Id.create(personString, Person.class), Id.create(vehicleString, Vehicle.class)));
			} else if (PersonLeavesVehicleEvent.EVENT_TYPE.equals(eventType)) {
				Id<Person> pId = Id.create(eventData.getAttribute(PersonLeavesVehicleEvent.ATTRIBUTE_PERSON), Person.class);
				Id<Vehicle> vId = Id.create(eventData.getAttribute(PersonLeavesVehicleEvent.ATTRIBUTE_VEHICLE), Vehicle.class);
				completeEvent(eventData, new PersonLeavesVehicleEvent(time, pId, vId));
			} else if (TeleportationArrivalEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new TeleportationArrivalEvent(
						time,
						Id.create(eventData.getAttribute(TeleportationArrivalEvent.ATTRIBUTE_PERSON), Person.class),
						Double.parseDouble(eventData.getAttribute(TeleportationArrivalEvent.ATTRIBUTE_DISTANCE)), eventData.getAttribute(TeleportationArrivalEvent.ATTRIBUTE_MODE)));
			} else if (VehicleArrivesAtFacilityEvent.EVENT_TYPE.equals(eventType)) {
				String delay = eventData.getAttribute(VehicleArrivesAtFacilityEvent.ATTRIBUTE_DELAY);
				completeEvent(eventData, new VehicleArrivesAtFacilityEvent(time, Id.create(eventData.getAttribute(VehicleArrivesAtFacilityEvent.ATTRIBUTE_VEHICLE), Vehicle.class), Id.create(eventData.getAttribute(VehicleArrivesAtFacilityEvent.ATTRIBUTE_FACILITY), TransitStopFacility.class), delay == null ? 0.0 : Double.parseDouble(delay)));
			} else if (VehicleDepartsAtFacilityEvent.EVENT_TYPE.equals(eventType)) {
				String delay = eventData.getAttribute(VehicleDepartsAtFacilityEvent.ATTRIBUTE_DELAY);
				completeEvent(eventData, new VehicleDepartsAtFacilityEvent(time, Id.create(eventData.getAttribute(VehicleArrivesAtFacilityEvent.ATTRIBUTE_VEHICLE), Vehicle.class), Id.create(eventData.getAttribute(VehicleArrivesAtFacilityEvent.ATTRIBUTE_FACILITY), TransitStopFacility.class), delay == null ? 0.0 : Double.parseDouble(delay)));
			} else if (TransitDriverStartsEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new TransitDriverStartsEvent(time, Id.create(eventData.getAttribute(TransitDriverStartsEvent.ATTRIBUTE_DRIVER_ID), Person.class), Id.create(eventData.getAttribute(TransitDriverStartsEvent.ATTRIBUTE_VEHICLE_ID), Vehicle.class), Id.create(eventData.getAttribute(TransitDriverStartsEvent.ATTRIBUTE_TRANSIT_LINE_ID), TransitLine.class), Id.create(eventData.getAttribute(TransitDriverStartsEvent.ATTRIBUTE_TRANSIT_ROUTE_ID), TransitRoute.class), Id.create(eventData.getAttribute(TransitDriverStartsEvent.ATTRIBUTE_DEPARTURE_ID), Departure.class)));
			} else if (BoardingDeniedEvent.EVENT_TYPE.equals(eventType)) {
				Id<Person> personId = Id.create(eventData.getAttribute(BoardingDeniedEvent.ATTRIBUTE_PERSON_ID), Person.class);
				Id<Vehicle> vehicleId = Id.create(eventData.getAttribute(BoardingDeniedEvent.ATTRIBUTE_VEHICLE_ID), Vehicle.class);
				completeEvent(eventData, new BoardingDeniedEvent(time, personId, vehicleId));
			} else if (AgentWaitingForPtEvent.EVENT_TYPE.equals(eventType)) {
				Id<Person> agentId = Id.create(eventData.getAttribute(AgentWaitingForPtEvent.ATTRIBUTE_AGENT), Person.class);
				Id<TransitStopFacility> waitStopId = Id.create(eventData.getAttribute(AgentWaitingForPtEvent.ATTRIBUTE_WAITSTOP), TransitStopFacility.class);
				Id<TransitStopFacility> destinationStopId = Id.create(eventData.getAttribute(AgentWaitingForPtEvent.ATTRIBUTE_DESTINATIONSTOP), TransitStopFacility.class);
				completeEvent(eventData, new AgentWaitingForPtEvent(time, agentId, waitStopId, destinationStopId));
			} else {
				GenericEvent event = new GenericEvent(eventType, time);
				for (int ii = 0; ii < eventData.xmlAttributes.getLength(); ii++) {
					String key = eventData.xmlAttributes.getLocalName(ii);
					if (key.equals("time") || key.equals("type")) {
						continue;
					}
					String value = eventData.xmlAttributes.getValue(ii);
					event.getAttributes().put(key, value);
				}
				MatsimEventsReader.CustomEventMapper cem = customEventMappers.get(eventType);
				if (cem != null) {
					completeEvent(eventData, cem.apply(event));
				} else {
					completeEvent(eventData, event);
				}
			}
		}
	}

}
