package org.zoodb.jdo.api.impl;

import org.zoodb.jdo.internal.Session;

public class DBStatistics {

	private final Session s; 
	
	public DBStatistics(Session s) {
		this.s = s;
	}

	/**
	 * 
	 * @return Number of written pages since the session was created. This includes pages that 
	 * are not written yet (commit pending) and pages that have been rolled back.
	 */
	public int getStoragePageWriteCount() {
		return s.getPrimaryNode().getStatsPageWriteCount();
	}

}
