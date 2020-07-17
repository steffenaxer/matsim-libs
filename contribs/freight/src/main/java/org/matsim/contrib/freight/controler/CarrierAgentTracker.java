package org.matsim.contrib.freight.controler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.*;
import org.matsim.api.core.v01.events.handler.*;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.Activity;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.Plan;
import org.matsim.contrib.freight.carrier.Carrier;
import org.matsim.contrib.freight.carrier.CarrierShipment;
import org.matsim.contrib.freight.carrier.Carriers;
import org.matsim.contrib.freight.carrier.ScheduledTour;
import org.matsim.contrib.freight.events.ShipmentDeliveredEvent;
import org.matsim.contrib.freight.events.ShipmentPickedUpEvent;
import org.matsim.contrib.freight.controler.CarrierAgent.CarrierDriverAgent;
import org.matsim.contrib.freight.events.eventsCreator.LSPEventCreator;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.algorithms.Vehicle2DriverEventHandler;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.scoring.ScoringFunction;

/**
 * This keeps track of all carrierAgents during simulation.
 * 
 * @author mzilske, sschroeder
 *
 */
public class CarrierAgentTracker implements ActivityStartEventHandler, ActivityEndEventHandler, PersonDepartureEventHandler, PersonArrivalEventHandler,
						     LinkEnterEventHandler, VehicleEntersTrafficEventHandler, VehicleLeavesTrafficEventHandler,
						     LinkLeaveEventHandler, PersonEntersVehicleEventHandler, PersonLeavesVehicleEventHandler
{
	private static final Logger log = Logger.getLogger( CarrierAgentTracker.class ) ;

	private final Carriers carriers;

	private final EventsManager eventsManager;
	private final LSPFreightControlerListener listener ;

	private final Vehicle2DriverEventHandler delegate = new Vehicle2DriverEventHandler();

	private final Collection<CarrierAgent> carrierAgents = new ArrayList<CarrierAgent>();
	
	private final Map<Id<Person>, CarrierAgent> driverAgentMap = new HashMap<Id<Person>, CarrierAgent>();

	private final Collection<LSPEventCreator> lspEventCreators;


	public CarrierAgentTracker( Carriers carriers, Network network, LSPFreightControlerListener listener, Collection<LSPEventCreator> creators ) {
		this.carriers = carriers;
		this.lspEventCreators = creators;
		createCarrierAgents(null);
		this.listener = listener;

		eventsManager = EventsUtils.createEventsManager();
		// yyyy this is not using the central events manager; I have no idea why this might work.  kai, oct'19

	}


	public CarrierAgentTracker(Carriers carriers, Network network, CarrierScoringFunctionFactory carrierScoringFunctionFactory) {
		log.warn( "calling ctor; carrierScoringFunctionFactory=" + carrierScoringFunctionFactory.getClass() );
		this.carriers = carriers;
		createCarrierAgents(carrierScoringFunctionFactory);

		eventsManager = EventsUtils.createEventsManager();
		// yyyy this is not using the central events manager; I have no idea why this might work.  kai, oct'19
		listener = null;
		lspEventCreators = null;
	}

	private void createCarrierAgents(CarrierScoringFunctionFactory carrierScoringFunctionFactory) {
		for (Carrier carrier : carriers.getCarriers().values()) {
			log.warn( "" );
			log.warn( "about to create scoring function for carrierId=" + carrier.getId() );
			ScoringFunction carrierScoringFunction = null ;
			if ( carrierScoringFunctionFactory != null ){
				carrierScoringFunction = carrierScoringFunctionFactory.createScoringFunction( carrier );
			}
			log.warn( "have now created scoring function for carrierId=" + carrier.getId() );
			log.warn( "" );
			CarrierAgent carrierAgent = new CarrierAgent(this, carrier, carrierScoringFunction, delegate);
			carrierAgents.add(carrierAgent);
		}
	}

	public EventsManager getEventsManager() {
		return eventsManager;
	}

	/**
	 * Returns the entire set of selected carrier plans.
	 * 
	 * @return collection of plans
	 * @see Plan, CarrierPlan
	 */
	public Collection<Plan> createPlans() {
		List<Plan> vehicleRoutes = new ArrayList<>();
		for (CarrierAgent carrierAgent : carrierAgents) {
			List<Plan> plansForCarrier = carrierAgent.createFreightDriverPlans();
			vehicleRoutes.addAll(plansForCarrier);
		}
		return vehicleRoutes;
	}

	/**
	 * Request all carrier agents to score their plans.
	 * 
	 */
	public void scoreSelectedPlans() {
//		log.warn("calling scoreSelectedPlans") ;
		for (Carrier carrier : carriers.getCarriers().values()) {
			CarrierAgent agent = findCarrierAgent(carrier.getId());
			agent.scoreSelectedPlan();
		}
	}

	@Override
	public void reset(int iteration) {
		delegate.reset(iteration);
	}

	private CarrierAgent findCarrierAgent(Id<Carrier> id) {
		for (CarrierAgent agent : carrierAgents) {
			if (agent.getId().equals(id)) {
				return agent;
			}
		}
		return null;
	}

	private void processEvent(Event event) {
		eventsManager.processEvent(event);
	}
	public void notifyEventHappened( Event event, Carrier carrier, Activity activity, ScheduledTour scheduledTour, Id<Person> driverId, int activityCounter ) {
		if ( lspEventCreators ==null ) return ;
		for(LSPEventCreator LSPEventCreator : lspEventCreators ) {
			Event customEvent = LSPEventCreator.createEvent(event, carrier, activity, scheduledTour, driverId, activityCounter);
			if(customEvent != null) {
				processEvent(customEvent);
			}
		}
	}

	/**
	 * Informs the world that a shipment has been picked up.
	 *
	 * <p>Is called by carrierAgent in charge of picking up shipments. It throws an ShipmentPickedupEvent which can be listened to
	 * with an ShipmentPickedUpListener.
	 *
	 * @param carrierId
	 * @param driverId
	 * @param shipment
	 * @param time
	 * @see ShipmentPickedUpEvent, ShipmentPickedUpEventHandler
	 */
	public void notifyPickedUp(Id<Carrier> carrierId, Id<Person> driverId, CarrierShipment shipment, double time) {
		processEvent(new ShipmentPickedUpEvent(carrierId, driverId, shipment, time));
	}

	public void notifyDelivered(Id<Carrier> carrierId, Id<Person> driverId, CarrierShipment shipment, double time) {
		processEvent(new ShipmentDeliveredEvent(carrierId, driverId, shipment,time));
	}

	@Override
	public void handleEvent(ActivityEndEvent event) {
		CarrierAgent carrierAgent = getCarrierAgent(event.getPersonId());
		if(carrierAgent == null) return;
		carrierAgent.handleEvent(event);
	}

	@Override
	public void handleEvent(LinkEnterEvent event) {
		CarrierAgent carrierAgent = getCarrierAgent(delegate.getDriverOfVehicle(event.getVehicleId()));
		if(carrierAgent == null) return;
		carrierAgent.handleEvent(event);
	}

	@Override
	public void handleEvent(ActivityStartEvent event) {
		CarrierAgent carrierAgent = getCarrierAgent(event.getPersonId());
		if(carrierAgent == null) return;
		carrierAgent.handleEvent(event);
	}


	@Override
	public void handleEvent(PersonArrivalEvent event) {
		CarrierAgent carrierAgent = getCarrierAgent(event.getPersonId());
		if(carrierAgent == null) return;
		carrierAgent.handleEvent(event);
	}

	@Override
	public void handleEvent(PersonDepartureEvent event) {
		CarrierAgent carrierAgent = getCarrierAgent(event.getPersonId());
		if(carrierAgent == null) return;
		carrierAgent.handleEvent(event);
	}

	private CarrierAgent getCarrierAgent(Id<Person> driverId) {
		if(driverAgentMap.containsKey(driverId)){
			return driverAgentMap.get(driverId);
		}
		for(CarrierAgent ca : carrierAgents){
			if(ca.getDriverIds().contains(driverId)){
				driverAgentMap.put(driverId, ca);
				return ca;
			}
		}
		return null;	
	}
	
	CarrierDriverAgent getDriver(Id<Person> driverId){
		CarrierAgent carrierAgent = getCarrierAgent(driverId);
		if(carrierAgent == null) throw new IllegalStateException("missing carrier agent. cannot find carrierAgent to driver " + driverId);
		return carrierAgent.getDriver(driverId);
	}

	@Override
	public void handleEvent(VehicleLeavesTrafficEvent event) {
		delegate.handleEvent(event);
		CarrierAgent carrierResourceAgent = getCarrierAgent(event.getPersonId() );
		if(carrierResourceAgent == null) return;
		carrierResourceAgent.handleEvent(event);
	}

	@Override
	public void handleEvent(VehicleEntersTrafficEvent event) {
		delegate.handleEvent(event);
		CarrierAgent carrierResourceAgent = getCarrierAgent(event.getPersonId() );
		if(carrierResourceAgent == null) return;
		carrierResourceAgent.handleEvent(event);
	}

	@Override
	public void handleEvent( LinkLeaveEvent event ) {
		CarrierAgent carrierResourceAgent = getCarrierAgent(delegate.getDriverOfVehicle(event.getVehicleId() ) );
		if(carrierResourceAgent == null) return;
		carrierResourceAgent.handleEvent(event);
	}

	@Override
	public void handleEvent(PersonEntersVehicleEvent event) {
		CarrierAgent carrierResourceAgent = getCarrierAgent(event.getPersonId() );
		if(carrierResourceAgent == null) return;
		carrierResourceAgent.handleEvent(event);
	}

	@Override
	public void handleEvent(PersonLeavesVehicleEvent event) {
		CarrierAgent carrierResourceAgent = getCarrierAgent(event.getPersonId() );
		if(carrierResourceAgent == null) return;
		carrierResourceAgent.handleEvent(event);
	}

}
