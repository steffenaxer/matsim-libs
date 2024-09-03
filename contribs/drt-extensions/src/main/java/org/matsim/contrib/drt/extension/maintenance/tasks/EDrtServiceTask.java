package org.matsim.contrib.drt.extension.maintenance.tasks;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.evrp.ETask;

/**
 * @author steffenaxer
 */
public class EDrtServiceTask extends DrtServiceTask implements ETask {

	private final double consumedEnergy;

	public EDrtServiceTask(double beginTime, double endTime, Link link, double consumedEnergy) {
		super(beginTime, endTime, link);
		this.consumedEnergy = consumedEnergy;
	}

	@Override
	public double getTotalEnergy() {
		return consumedEnergy;
	}
}

