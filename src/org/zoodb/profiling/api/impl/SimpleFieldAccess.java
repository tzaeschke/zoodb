package org.zoodb.profiling.api.impl;

public class SimpleFieldAccess {
	
	private int idx;
	private int rCount;
	private int wCount;
	
	
	public int getIdx() {
		return idx;
	}
	public void setIdx(int idx) {
		this.idx = idx;
	}
	public int getrCount() {
		return rCount;
	}
	public void setrCount(int rCount) {
		this.rCount = rCount;
	}
	public int getwCount() {
		return wCount;
	}
	public void setwCount(int wCount) {
		this.wCount = wCount;
	}
	
	public void incR() {
		rCount++;
	}
	public void incW() {
		wCount++;
	}
}