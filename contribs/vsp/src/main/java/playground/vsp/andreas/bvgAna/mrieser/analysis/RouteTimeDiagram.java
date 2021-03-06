/* *********************************************************************** *
 * project: org.matsim.*
 * RouteTimeDiagram.java
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2009 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */

package playground.vsp.andreas.bvgAna.mrieser.analysis;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.matsim.api.core.v01.Id;
import org.matsim.core.api.experimental.events.VehicleArrivesAtFacilityEvent;
import org.matsim.core.api.experimental.events.VehicleDepartsAtFacilityEvent;
import org.matsim.core.api.experimental.events.handler.VehicleArrivesAtFacilityEventHandler;
import org.matsim.core.api.experimental.events.handler.VehicleDepartsAtFacilityEventHandler;
import org.matsim.core.utils.collections.Tuple;
import org.matsim.pt.transitSchedule.api.Departure;
import org.matsim.pt.transitSchedule.api.TransitRoute;
import org.matsim.pt.transitSchedule.api.TransitRouteStop;
import org.matsim.pt.transitSchedule.api.TransitStopFacility;
import org.matsim.vehicles.Vehicle;

/**
 * Collects data to create Route-Time-Diagrams based on the actual simulation.
 * A Route-Time-Diagram shows the position along one transit route of one or
 * more vehicles over the lapse of time.
 *
 * @author mrieser
 */
public class RouteTimeDiagram implements VehicleArrivesAtFacilityEventHandler, VehicleDepartsAtFacilityEventHandler {

	/**
	 * Map containing for each vehicle a list of positions, stored as StopFacility Ids and the time.
	 */
	private final Map<Id<Vehicle>, List<Tuple<Id<TransitStopFacility>, Double>>> positions = new HashMap<>();

	public void handleEvent(final VehicleArrivesAtFacilityEvent event) {
		List<Tuple<Id<TransitStopFacility>, Double>> list = this.positions.computeIfAbsent(event.getVehicleId(), k -> new ArrayList<>());
		list.add(new Tuple<>(event.getFacilityId(), event.getTime()));
	}

	public void handleEvent(final VehicleDepartsAtFacilityEvent event) {
		List<Tuple<Id<TransitStopFacility>, Double>> list = this.positions.computeIfAbsent(event.getVehicleId(), k -> new ArrayList<>());
		list.add(new Tuple<>(event.getFacilityId(), event.getTime()));
	}

	public void reset(final int iteration) {
		this.positions.clear();
	}

	public void writeData() {
		for (List<Tuple<Id<TransitStopFacility>, Double>> list : this.positions.values()) {
			for (Tuple<Id<TransitStopFacility>, Double> info : list) {
				System.out.println(info.getFirst().toString() + "\t" + info.getSecond().toString());
			}
			System.out.println();
		}
	}

	public void createGraph(final String filename, final TransitRoute route) {

		HashMap<Id<TransitStopFacility>, Integer> stopIndex = new HashMap<>();
		int idx = 0;
		for (TransitRouteStop stop : route.getStops()) {
			stopIndex.put(stop.getStopFacility().getId(), idx);
			idx++;
		}

		HashSet<Id<Vehicle>> vehicles = new HashSet<>();
		for (Departure dep : route.getDepartures().values()) {
			vehicles.add(dep.getVehicleId());
		}

		XYSeriesCollection dataset = new XYSeriesCollection();
		int numSeries = 0;
		double earliestTime = Double.POSITIVE_INFINITY;
		double latestTime = Double.NEGATIVE_INFINITY;

		for (Map.Entry<Id<Vehicle>, List<Tuple<Id<TransitStopFacility>, Double>>> entry : this.positions.entrySet()) {
			if (vehicles.contains(entry.getKey())) {
				XYSeries series = new XYSeries("t", false, true);
				for (Tuple<Id<TransitStopFacility>, Double> pos : entry.getValue()) {
					Integer stopIdx = stopIndex.get(pos.getFirst());
					if (stopIdx != null) {
						double time = pos.getSecond();
						series.add(stopIdx.intValue(), time);
						if (time < earliestTime) {
							earliestTime = time;
						}
						if (time > latestTime) {
							latestTime = time;
						}
					}
				}
				dataset.addSeries(series);
				numSeries++;

			}
		}

		JFreeChart c = ChartFactory.createXYLineChart("Route-Time Diagram, Route = " + route.getId(), "stops", "time",
				dataset, PlotOrientation.VERTICAL,
				false, // legend?
				false, // tooltips?
				false // URLs?
				);
		c.setBackgroundPaint(new Color(1.0f, 1.0f, 1.0f, 1.0f));

		XYPlot p  = (XYPlot) c.getPlot();

		p.getRangeAxis().setInverted(true);
		p.getRangeAxis().setRange(earliestTime, latestTime);
		XYItemRenderer renderer = p.getRenderer();
		for (int i = 0; i < numSeries; i++) {
			renderer.setSeriesPaint(i, Color.black);
		}

		try {
			ChartUtils.saveChartAsPNG(new File(filename), c, 1024, 768, null, true, 9);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
