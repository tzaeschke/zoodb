package org.zoodb.profiling.api.impl;

public class Trx {
	
	private String id;
	
	private long start;
	private long end;
	
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
	}

}