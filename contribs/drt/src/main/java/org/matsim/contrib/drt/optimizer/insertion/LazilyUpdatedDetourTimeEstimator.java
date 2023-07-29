package org.matsim.contrib.drt.optimizer.insertion;

import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Node;
import org.matsim.contrib.dvrp.path.VrpPaths;
import org.matsim.contrib.zone.skims.LazyTravelTimeMatrix;
import org.matsim.contrib.zone.skims.TravelTimeMatrix;
import org.matsim.core.router.util.TravelTime;

import static org.matsim.contrib.dvrp.path.VrpPaths.FIRST_LINK_TT;

/**
 * @author steffenaxer
 */
public class LazilyUpdatedDetourTimeEstimator implements DetourTimeEstimator {

	private final TravelTime travelTime;
	private final TravelTimeMatrix freeSpeedTravelTimeMatrix;
	private final LazyTravelTimeMatrix lazyTravelTimeMatrix;
	private final double speedFactor;

	static int lazyCounter = 0;
	static int freeSpeedCounter = 0;

	LazilyUpdatedDetourTimeEstimator(double speedFactor, TravelTimeMatrix freeSpeedTravelTimeMatrix, LazyTravelTimeMatrix lazyTravelTimeMatrix, TravelTime travelTime) {
		this.speedFactor = speedFactor;
		this.freeSpeedTravelTimeMatrix = freeSpeedTravelTimeMatrix;
		this.lazyTravelTimeMatrix = lazyTravelTimeMatrix;
		this.travelTime = travelTime;
	}

	public static LazilyUpdatedDetourTimeEstimator create(double speedFactor, TravelTimeMatrix freeSpeedTravelTimeMatrix, LazyTravelTimeMatrix lazyTravelTimeMatrix, TravelTime travelTime)
	{
		return new LazilyUpdatedDetourTimeEstimator(speedFactor,freeSpeedTravelTimeMatrix,lazyTravelTimeMatrix,travelTime);
	}

	@Override
	public double estimateTime(Link from, Link to, double departureTime) {

		if (from == to) {
			return 0;
		}

		double duration = FIRST_LINK_TT;
		duration += this.getTravelTime(from.getToNode(), to.getFromNode(), departureTime + duration);
		duration += VrpPaths.getLastLinkTT(travelTime, to, departureTime + duration);
		return duration;
	}

	double getTravelTime(Node fromNode, Node toNode, double departureTime) {
		if (this.lazyTravelTimeMatrix.getTravelTime(fromNode, toNode, departureTime).isDefined()) {
			this.lazyCounter++;
			return this.lazyTravelTimeMatrix.getTravelTime(fromNode, toNode, departureTime).seconds();
		} else {
			this.freeSpeedCounter++;
			return this.freeSpeedTravelTimeMatrix.getTravelTime(fromNode, toNode, departureTime) / speedFactor;
		}
	}

}
