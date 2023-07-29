package org.matsim.contrib.zone.skims;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.dvrp.run.AbstractDvrpModeModule;
import org.matsim.contrib.dvrp.run.DvrpConfigGroup;
import org.matsim.core.config.groups.GlobalConfigGroup;
import org.matsim.core.config.groups.QSimConfigGroup;

/**
 * @author steffenaxer
 */
public class LazyTravelTimeMatrixModule extends AbstractDvrpModeModule {
	@Inject
	private DvrpConfigGroup dvrpConfigGroup;

	LazyTravelTimeMatrixModule(String mode)
	{
		super(mode);
	}
	@Override
	public void install() {
		bindModal(LazyTravelTimeMatrix.class).toProvider(modalProvider(
			getter -> new LazyTravelTimeMatrixImpl(getter.getModal(Network.class),dvrpConfigGroup.getTravelTimeMatrixParams()))).in(Singleton.class);
	}
}
