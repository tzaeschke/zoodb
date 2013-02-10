package org.zoodb.profiling.api.impl;

import java.util.Iterator;

import org.zoodb.profiling.api.IPathManager;

public class Trx {
	
	private String id;
	
	private long start;
	private long end;
	
	/**
	 * Amount of time this transaction spend for activating objects
	 */
	private long activationTime;
	
	private boolean rollbacked;

	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public boolean isRollbacked() {
		return rollbacked;
	}
	public void setRollbacked(boolean rollbacked) {
		this.rollbacked = rollbacked;
		
		if (rollbacked) {
			//we do not analyze paths for rollbacked trx			
			removeActivations();
		}
	}
	
	public void updateActivationTime(long time) {
		activationTime += time;
	}
	public long getActivationTime() {
		return activationTime;
	}
	
	private void removeActivations() {
		IPathManager pm = ProfilingManager.getInstance().getPathManager();
		Iterator<Class<?>> iter = pm.getClassIterator();
		
		ActivationArchive pcArchive = null;
		int removalCount = 0;
		while (iter.hasNext()) {
			pcArchive = pm.getArchive(iter.next());
			
			removalCount += pcArchive.removeAllForTrx(this);
		}
		
	}

}