package org.matsim.contrib.drt.extension.maintenance;

import org.matsim.api.core.v01.network.Link;
import org.matsim.contrib.drt.extension.operations.operationFacilities.OperationFacility;
import org.matsim.contrib.drt.extension.operations.shifts.schedule.OperationalStop;
import org.matsim.contrib.drt.schedule.DrtTaskType;
import org.matsim.contrib.dvrp.schedule.DefaultStayTask;

import static org.matsim.contrib.drt.schedule.DrtTaskBaseType.STAY;

/**
 * @author steffenaxer
 */
public class MaintenanceTask extends DefaultStayTask implements OperationalStop {

	public static final DrtTaskType TYPE = new DrtTaskType("MAINTENANCE", STAY);

	private final OperationFacility facility;

    public MaintenanceTask(double beginTime, double endTime, Link link, OperationFacility facility) {
        super(TYPE, beginTime, endTime, link);
        this.facility = facility;
    }

    @Override
    public OperationFacility getFacility() {
        return facility;
    }

}
