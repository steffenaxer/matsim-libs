package org.matsim.contrib.zone.skims;

import com.google.inject.Inject;
import org.matsim.core.controler.events.IterationStartsEvent;
import org.matsim.core.controler.listener.IterationStartsListener;

/**
 * @author steffenaxer
 */
public class TimeDependentMatrixUpdater implements IterationStartsListener {
	private static int UPDATE_INTERVAL = 2;
	TravelTimeMatrix travelTimeMatrix;

	@Inject
	TimeDependentMatrixUpdater(TravelTimeMatrix travelTimeMatrix) {
		this.travelTimeMatrix = travelTimeMatrix;
	}

	@Override
	public void notifyIterationStarts(IterationStartsEvent event) {
		if (this.travelTimeMatrix instanceof TimeDependentTravelTimeMatrix timeDependentTravelTimeMatrix
				&& event.getIteration() % UPDATE_INTERVAL == 0
				&& event.getIteration() > 0) {
			timeDependentTravelTimeMatrix.update();
		}
	}
}
