package org.matsim.contrib.drt.extension.services.tasks;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.edrt.schedule.EDrtTaskFactoryImpl;

/**
 * @author steffenaxer
 */
public class EDrtServiceTaskFactoryImpl extends EDrtTaskFactoryImpl implements ServiceTaskFactory {

	@Override
	public DrtServiceTask createServiceTask(double beginTime, double endTime, Link link) {
		return new EDrtServiceTask(beginTime,endTime,link,0);
	}

}
