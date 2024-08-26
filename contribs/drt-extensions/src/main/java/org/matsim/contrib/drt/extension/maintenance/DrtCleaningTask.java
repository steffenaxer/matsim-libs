package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.extension.operations.shifts.schedule.OperationalStop;
import org.matsim.contrib.drt.passenger.AcceptedDrtRequest;
import org.matsim.contrib.drt.schedule.DrtStopTask;
import org.matsim.contrib.drt.schedule.DrtTaskType;
import org.matsim.contrib.dvrp.optimizer.Request;
import org.matsim.contrib.dvrp.schedule.DefaultStayTask;

import java.util.Collections;
import java.util.Map;

import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.STAY;
import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.STOP;

/**
 * @author steffenaxer
 */
public class DrtCleaningTask extends DefaultStayTask implements OperationalStop {

	public static final DrtTaskType TYPE = new DrtTaskType("CLEANING", STAY);

	private final OperationFacility facility;

    public DrtCleaningTask(double beginTime, double endTime, Link link, OperationFacility facility) {
        super(TYPE, beginTime, endTime, link);
        this.facility = facility;
    }

    @Override
    public OperationFacility getFacility() {
        return facility;
    }
}
