package org.matsim.contrib.zone.skims;

import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.contrib.zone.SquareGridSystem;
import org.matsim.contrib.zone.ZonalSystems;
import org.matsim.core.utils.misc.OptionalTime;

import java.util.NoSuchElementException;


/**
 * @author steffenaxer
 */
public class LazyTravelTimeMatrixImpl implements LazyTravelTimeMatrix {
	private final Matrix matrix;
	private final SquareGridSystem gridSystem;
	private double alpha = 0.25;

	public LazyTravelTimeMatrixImpl(Network dvrpNetwork, DvrpTravelTimeMatrixParams params) {
		this.gridSystem = new SquareGridSystem(dvrpNetwork.getNodes().values(), params.cellSize);
		this.matrix = new Matrix(ZonalSystems.computeMostCentralNodes(dvrpNetwork.getNodes().values(), this.gridSystem).keySet());
	}

	@Override
	public OptionalTime getTravelTime(Node fromNode, Node toNode, double departureTime) {
		try {
			int value = this.matrix.get(this.gridSystem.getZone(fromNode), this.gridSystem.getZone(toNode));
			return OptionalTime.defined(value);
		} catch (NoSuchElementException e)
		{
			return OptionalTime.undefined();
		}
	}
	@Override
	public void setTravelTime(Node fromNode, Node toNode, double routeEstimate, double departureTime) {
		OptionalTime currentTravelTimeEstimate = this.getTravelTime(fromNode, toNode, departureTime);

		if (currentTravelTimeEstimate.isDefined()) {
			double value = currentTravelTimeEstimate.seconds() * (1 - this.alpha) + this.alpha * routeEstimate;
			this.matrix.set(this.gridSystem.getZone(fromNode), this.gridSystem.getZone(toNode), value);
		} else {
			this.matrix.set(this.gridSystem.getZone(fromNode), this.gridSystem.getZone(toNode), routeEstimate);
		}
	}
}
