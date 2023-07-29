package org.matsim.contrib.zone.skims;

import org.matsim.api.core.v01.network.Node;
import org.matsim.core.utils.misc.OptionalTime;

/**
 * @author steffenaxer
 */
public interface LazyTravelTimeMatrix {
	 OptionalTime getTravelTime(Node fromNode, Node toNode, double departureTime);
	 void setTravelTime(Node fromNode, Node toNode, double travelTime, double departureTime);
}
