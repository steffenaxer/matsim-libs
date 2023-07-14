package org.matsim.core.events;

import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.*;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.api.experimental.events.*;
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
 * @author steffenaxer
 */
public class ParallelEventsReaderXMLv1 extends MatsimXmlEventsParser {
	static public final String EVENT = "event";
	static public final String EVENTS = "events";
	private static final int THREADS_LIMIT = 4;
	public static String CLOSING_MARKER = UUID.randomUUID().toString();
	private final Map<String, MatsimEventsReader.CustomEventMapper> customEventMappers = new HashMap<>();
	final BlockingQueue<EventData> eventsQueue = new LinkedBlockingQueue<>();
	final BlockingQueue<CompletableFuture<Event>> futureEventsQueue = new LinkedBlockingQueue<>();
	final EventsManager eventsManager;
	Thread[] workerThreads;
	Thread inserterThread;

	public ParallelEventsReaderXMLv1(EventsManager eventsManager) {
		this.eventsManager = eventsManager;
		this.initializeWorkerThreads();
		this.initializeInserterThread();
	}

	void initializeWorkerThreads() {
		workerThreads = new Thread[THREADS_LIMIT];
		for (int i = 0; i < THREADS_LIMIT; i++) {
			MatsimEventsWorker runner =
					new MatsimEventsWorker(this.eventsQueue, this.customEventMappers);
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
		thread.setName(MatsimEventsWorker.class.toString());
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
			CompletableFuture<Event> futureEvent = new CompletableFuture<>();
			EventData eventData = new EventData(futureEvent, name, new AttributesImpl(atts));
			try {
				this.eventsQueue.put(eventData);
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
				EventData closingData = new EventData(e, CLOSING_MARKER, null);
				this.eventsQueue.put(closingData);
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

	record EventData(CompletableFuture<Event> futureEvent, String name, Attributes atts) {
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
					if (eventData.name.equals(CLOSING_MARKER)) {
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
			Attributes atts = eventData.atts;
			double time = Double.parseDouble(atts.getValue("time"));
			String eventType = atts.getValue("type");

			// === material related to wait2link below here ===
			if (LinkLeaveEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new LinkLeaveEvent(time,
						Id.create(atts.getValue(LinkLeaveEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						Id.create(atts.getValue(LinkLeaveEvent.ATTRIBUTE_LINK), Link.class)
						// had driver id in previous version
				));
			} else if (LinkEnterEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new LinkEnterEvent(time,
						Id.create(atts.getValue(LinkEnterEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						Id.create(atts.getValue(LinkEnterEvent.ATTRIBUTE_LINK), Link.class)
						// had driver id in previous version
				));
			} else if (VehicleEntersTrafficEvent.EVENT_TYPE.equals(eventType)) {
				// (this is the new version, marked by the new events name)

				completeEvent(eventData, new VehicleEntersTrafficEvent(time,
						Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_LINK), Link.class),
						Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_NETWORKMODE),
						Double.parseDouble(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION))
				));
			} else if ("wait2link".equals(eventType)) {
				// (this is the old version, marked by the old events name)

				// retrofit vehicle Id:
				Id<Vehicle> vehicleId;
				if (atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE) != null) {
					vehicleId = Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class);
				} else {
					// for the old events type, we set the vehicle id to the driver id if the vehicle id does not exist:
					vehicleId = Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Vehicle.class);
				}
				// retrofit position:
				double position;
				if (atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION) != null) {
					position = Double.parseDouble(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_POSITION));
				} else {
					position = 1.0;
				}
				completeEvent(eventData, new VehicleEntersTrafficEvent(time,
						Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_LINK), Link.class),
						vehicleId,
						atts.getValue(VehicleEntersTrafficEvent.ATTRIBUTE_NETWORKMODE),
						position
				));
			} else if (VehicleLeavesTrafficEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new VehicleLeavesTrafficEvent(time,
						Id.create(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_DRIVER), Person.class),
						Id.create(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_LINK), Link.class),
						atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_VEHICLE) == null ? null : Id.create(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_VEHICLE), Vehicle.class),
						atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_NETWORKMODE),
						Double.parseDouble(atts.getValue(VehicleLeavesTrafficEvent.ATTRIBUTE_POSITION))
				));
			}
			// === material related to wait2link above here
			else if (ActivityEndEvent.EVENT_TYPE.equals(eventType)) {
				Coord coord = null;
				if (atts.getValue(Event.ATTRIBUTE_X) != null) {
					double xx = Double.parseDouble(atts.getValue(Event.ATTRIBUTE_X));
					double yy = Double.parseDouble(atts.getValue(Event.ATTRIBUTE_Y));
					coord = new Coord(xx, yy);
				}
				completeEvent(eventData, new ActivityEndEvent(
						time,
						Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(atts.getValue(HasLinkId.ATTRIBUTE_LINK), Link.class),
						atts.getValue(HasFacilityId.ATTRIBUTE_FACILITY) == null ? null : Id.create(atts.getValue(HasFacilityId.ATTRIBUTE_FACILITY),
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
				completeEvent(eventData, new ActivityStartEvent(
						time,
						Id.create(atts.getValue(HasPersonId.ATTRIBUTE_PERSON), Person.class),
						Id.create(atts.getValue(HasLinkId.ATTRIBUTE_LINK), Link.class),
						atts.getValue(HasFacilityId.ATTRIBUTE_FACILITY) == null ? null : Id.create(atts.getValue(
								HasFacilityId.ATTRIBUTE_FACILITY), ActivityFacility.class),
						atts.getValue(ActivityStartEvent.ATTRIBUTE_ACTTYPE),
						coord));
			} else if (PersonArrivalEvent.EVENT_TYPE.equals(eventType)) {
				String legMode = atts.getValue(PersonArrivalEvent.ATTRIBUTE_LEGMODE);
				String mode = legMode == null ? null : legMode.intern();
				completeEvent(eventData, new PersonArrivalEvent(time, Id.create(atts.getValue(PersonArrivalEvent.ATTRIBUTE_PERSON), Person.class), Id.create(atts.getValue(PersonArrivalEvent.ATTRIBUTE_LINK), Link.class), mode));
			} else if (PersonDepartureEvent.EVENT_TYPE.equals(eventType)) {
				String legMode = atts.getValue(PersonDepartureEvent.ATTRIBUTE_LEGMODE);
				String canonicalLegMode = legMode == null ? null : legMode.intern();
				String routingMode = atts.getValue(PersonDepartureEvent.ATTRIBUTE_ROUTING_MODE);
				String canonicalRoutingMode = routingMode == null ? null : routingMode.intern();
				completeEvent(eventData, new PersonDepartureEvent(time, Id.create(atts.getValue(PersonDepartureEvent.ATTRIBUTE_PERSON), Person.class), Id.create(atts.getValue(PersonDepartureEvent.ATTRIBUTE_LINK), Link.class), canonicalLegMode, canonicalRoutingMode));
			} else if (PersonStuckEvent.EVENT_TYPE.equals(eventType)) {
				String legMode = atts.getValue(PersonStuckEvent.ATTRIBUTE_LEGMODE);
				String mode = legMode == null ? null : legMode.intern();
				String linkIdString = atts.getValue(PersonStuckEvent.ATTRIBUTE_LINK);
				Id<Link> linkId = linkIdString == null ? null : Id.create(linkIdString, Link.class); // linkId is optional
				completeEvent(eventData, new PersonStuckEvent(time, Id.create(atts.getValue(PersonStuckEvent.ATTRIBUTE_PERSON), Person.class), linkId, mode));
			} else if (VehicleAbortsEvent.EVENT_TYPE.equals(eventType)) {
				String linkIdString = atts.getValue(VehicleAbortsEvent.ATTRIBUTE_LINK);
				Id<Link> linkId = linkIdString == null ? null : Id.create(linkIdString, Link.class);
				completeEvent(eventData, new VehicleAbortsEvent(time, Id.create(atts.getValue(VehicleAbortsEvent.ATTRIBUTE_VEHICLE), Vehicle.class), linkId));
			} else if (PersonMoneyEvent.EVENT_TYPE.equals(eventType) || "agentMoney".equals(eventType)) {
				completeEvent(eventData, new PersonMoneyEvent(time, Id.create(atts.getValue(PersonMoneyEvent.ATTRIBUTE_PERSON), Person.class), Double.parseDouble(atts.getValue(PersonMoneyEvent.ATTRIBUTE_AMOUNT)), atts.getValue(PersonMoneyEvent.ATTRIBUTE_PURPOSE), atts.getValue(PersonMoneyEvent.ATTRIBUTE_TRANSACTION_PARTNER)));
			} else if (PersonScoreEvent.EVENT_TYPE.equals(eventType) || "personScore".equals(eventType)) {
				completeEvent(eventData, new PersonScoreEvent(time, Id.create(atts.getValue(PersonScoreEvent.ATTRIBUTE_PERSON), Person.class), Double.parseDouble(atts.getValue(PersonScoreEvent.ATTRIBUTE_AMOUNT)), atts.getValue(PersonScoreEvent.ATTRIBUTE_KIND)));
			} else if (PersonEntersVehicleEvent.EVENT_TYPE.equals(eventType)) {
				String personString = atts.getValue(PersonEntersVehicleEvent.ATTRIBUTE_PERSON);
				String vehicleString = atts.getValue(PersonEntersVehicleEvent.ATTRIBUTE_VEHICLE);
				completeEvent(eventData, new PersonEntersVehicleEvent(time, Id.create(personString, Person.class), Id.create(vehicleString, Vehicle.class)));
			} else if (PersonLeavesVehicleEvent.EVENT_TYPE.equals(eventType)) {
				Id<Person> pId = Id.create(atts.getValue(PersonLeavesVehicleEvent.ATTRIBUTE_PERSON), Person.class);
				Id<Vehicle> vId = Id.create(atts.getValue(PersonLeavesVehicleEvent.ATTRIBUTE_VEHICLE), Vehicle.class);
				completeEvent(eventData, new PersonLeavesVehicleEvent(time, pId, vId));
			} else if (TeleportationArrivalEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new TeleportationArrivalEvent(
						time,
						Id.create(atts.getValue(TeleportationArrivalEvent.ATTRIBUTE_PERSON), Person.class),
						Double.parseDouble(atts.getValue(TeleportationArrivalEvent.ATTRIBUTE_DISTANCE)), atts.getValue(TeleportationArrivalEvent.ATTRIBUTE_MODE)));
			} else if (VehicleArrivesAtFacilityEvent.EVENT_TYPE.equals(eventType)) {
				String delay = atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_DELAY);
				completeEvent(eventData, new VehicleArrivesAtFacilityEvent(time, Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_VEHICLE), Vehicle.class), Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_FACILITY), TransitStopFacility.class), delay == null ? 0.0 : Double.parseDouble(delay)));
			} else if (VehicleDepartsAtFacilityEvent.EVENT_TYPE.equals(eventType)) {
				String delay = atts.getValue(VehicleDepartsAtFacilityEvent.ATTRIBUTE_DELAY);
				completeEvent(eventData, new VehicleDepartsAtFacilityEvent(time, Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_VEHICLE), Vehicle.class), Id.create(atts.getValue(VehicleArrivesAtFacilityEvent.ATTRIBUTE_FACILITY), TransitStopFacility.class), delay == null ? 0.0 : Double.parseDouble(delay)));
			} else if (TransitDriverStartsEvent.EVENT_TYPE.equals(eventType)) {
				completeEvent(eventData, new TransitDriverStartsEvent(time, Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_DRIVER_ID), Person.class), Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_VEHICLE_ID), Vehicle.class), Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_TRANSIT_LINE_ID), TransitLine.class), Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_TRANSIT_ROUTE_ID), TransitRoute.class), Id.create(atts.getValue(TransitDriverStartsEvent.ATTRIBUTE_DEPARTURE_ID), Departure.class)));
			} else if (BoardingDeniedEvent.EVENT_TYPE.equals(eventType)) {
				Id<Person> personId = Id.create(atts.getValue(BoardingDeniedEvent.ATTRIBUTE_PERSON_ID), Person.class);
				Id<Vehicle> vehicleId = Id.create(atts.getValue(BoardingDeniedEvent.ATTRIBUTE_VEHICLE_ID), Vehicle.class);
				completeEvent(eventData, new BoardingDeniedEvent(time, personId, vehicleId));
			} else if (AgentWaitingForPtEvent.EVENT_TYPE.equals(eventType)) {
				Id<Person> agentId = Id.create(atts.getValue(AgentWaitingForPtEvent.ATTRIBUTE_AGENT), Person.class);
				Id<TransitStopFacility> waitStopId = Id.create(atts.getValue(AgentWaitingForPtEvent.ATTRIBUTE_WAITSTOP), TransitStopFacility.class);
				Id<TransitStopFacility> destinationStopId = Id.create(atts.getValue(AgentWaitingForPtEvent.ATTRIBUTE_DESTINATIONSTOP), TransitStopFacility.class);
				completeEvent(eventData, new AgentWaitingForPtEvent(time, agentId, waitStopId, destinationStopId));
			} else {
				GenericEvent event = new GenericEvent(eventType, time);
				for (int ii = 0; ii < atts.getLength(); ii++) {
					String key = atts.getLocalName(ii);
					if (key.equals("time") || key.equals("type")) {
						continue;
					}
					String value = atts.getValue(ii);
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
