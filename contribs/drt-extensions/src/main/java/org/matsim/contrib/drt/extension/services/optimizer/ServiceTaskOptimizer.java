package org.matsim.contrib.drt.extension.services.optimizer;

import org.matsim.contrib.drt.extension.services.dispatcher.ServiceTaskDispatcher;
import org.matsim.contrib.drt.extension.operations.shifts.optimizer.ShiftDrtOptimizer;
import org.matsim.contrib.drt.optimizer.DrtOptimizer;
import org.matsim.contrib.dvrp.fleet.DvrpVehicle;
import org.matsim.contrib.dvrp.optimizer.Request;
import org.matsim.contrib.dvrp.schedule.ScheduleTimingUpdater;
import org.matsim.core.mobsim.framework.events.MobsimBeforeSimStepEvent;
import org.matsim.core.mobsim.framework.events.MobsimInitializedEvent;
import org.matsim.core.mobsim.framework.listeners.MobsimInitializedListener;

/**
 * @author steffenaxer
 */
public class ServiceTaskOptimizer implements DrtOptimizer, MobsimInitializedListener {
	private final DrtOptimizer delegate;
	private final ScheduleTimingUpdater scheduleTimingUpdater;
	private final ServiceTaskDispatcher serviceTaskDispatcher;

	public ServiceTaskOptimizer(ServiceTaskDispatcher serviceTaskDispatcher, DrtOptimizer delegate, ScheduleTimingUpdater scheduleTimingUpdater) {
		this.delegate = delegate;
		this.scheduleTimingUpdater = scheduleTimingUpdater;
		this.serviceTaskDispatcher = serviceTaskDispatcher;
	}

	@Override
	public void requestSubmitted(Request request) {
		this.delegate.requestSubmitted(request);
	}

	@Override
	public void nextTask(DvrpVehicle vehicle) {
		scheduleTimingUpdater.updateBeforeNextTask(vehicle);
		delegate.nextTask(vehicle);
	}

	@Override
	public void notifyMobsimBeforeSimStep(MobsimBeforeSimStepEvent e) {
		this.serviceTaskDispatcher.dispatch(e.getSimulationTime());
		this.delegate.notifyMobsimBeforeSimStep(e);
	}

	@Override
	public void notifyMobsimInitialized(MobsimInitializedEvent e) {
		if(this.delegate instanceof ShiftDrtOptimizer shiftDrtOptimizer)
		{
			shiftDrtOptimizer.notifyMobsimInitialized(e);
		}
	}
}
