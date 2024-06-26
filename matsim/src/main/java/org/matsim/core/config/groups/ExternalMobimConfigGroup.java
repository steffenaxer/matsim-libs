/* *********************************************************************** *
 * project: org.matsim.*
 * SimulationConfigGroup.java
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2007 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */

package org.matsim.core.config.groups;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.core.config.ReflectiveConfigGroup;
import org.matsim.core.utils.misc.OptionalTime;
import org.matsim.core.utils.misc.Time;

public final class ExternalMobimConfigGroup extends ReflectiveConfigGroup {

	@SuppressWarnings("unused")
	private final static Logger log = LogManager.getLogger(ExternalMobimConfigGroup.class);

	public static final String GROUP_NAME = "externalMobsim";

	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String EXTERNAL_EXE = "externalExe";
	private static final String TIMEOUT = "timeout";

	private OptionalTime startTime = OptionalTime.undefined();
	private OptionalTime endTime = OptionalTime.undefined();
	private String externalExe = null;
	private int timeOut = 3600;

	public ExternalMobimConfigGroup() {
		super(GROUP_NAME);
	}

	
	@Override
	public final Map<String, String> getComments() {
		Map<String,String> map = super.getComments();
		return map ;
	}


	@StringSetter(START_TIME)
	public void setStartTime(final String startTime) {
		this.startTime = Time.parseOptionalTime(startTime);
	}
	public void setStartTime(final double startTime) {
		this.startTime = OptionalTime.defined(startTime);
	}

	@StringGetter(START_TIME)
	String getStartTimeAsString() {
		return Time.writeTime(this.startTime) ;
	}
	public OptionalTime getStartTime() {
		return this.startTime;
	}

	@StringSetter(END_TIME)
	public void setEndTime(final String endTime) {
		this.setEndTime( Time.parseTime(endTime) );
	}
	public void setEndTime(final double endTime) {
		this.endTime = OptionalTime.defined(endTime);
	}

	@StringGetter(END_TIME)
	String getEndTimeAsString() {
		return Time.writeTime(this.endTime ) ;
	}
	public OptionalTime getEndTime() {
		return this.endTime;
	}

	@StringSetter( EXTERNAL_EXE )
	public void setExternalExe(final String externalExe) {
		this.externalExe = externalExe;
	}
	@StringGetter( EXTERNAL_EXE )
	public String getExternalExe() {
		return this.externalExe;
	}
	@StringSetter( TIMEOUT ) 	
	public void setExternalTimeOut(final int timeOut) {
		this.timeOut = timeOut;
	}
	@StringGetter( TIMEOUT ) 	
	public int getExternalTimeOut() {
		return this.timeOut;
	}

}
