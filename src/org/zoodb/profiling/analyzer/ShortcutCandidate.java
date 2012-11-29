package org.zoodb.profiling.analyzer;

import java.util.LinkedList;
import java.util.List;

public class ShortcutCandidate {
	
	private Class<?> start;
	private Class<?> end;
	
	private List<Class<?>> intermediates;
	private List<Long> intermediateSize;
	

	public ShortcutCandidate() {
		intermediates = new LinkedList<Class<?>>();
		intermediateSize = new LinkedList<Long>();
	}
	
	public Class<?> getStart() {
		return start;
	}

	public void setStart(Class<?> start) {
		this.start = start;
	}

	public List<Class<?>> getIntermediates() {
		return intermediates;
	}

	public void setIntermediates(List<Class<?>> intermediates) {
		this.intermediates = intermediates;
	}

	public List<Long> getIntermediateSize() {
		return intermediateSize;
	}

	public void setIntermediateSize(List<Long> intermediateSize) {
		this.intermediateSize = intermediateSize;
	}

	public Class<?> getEnd() {
		return end;
	}

	public void setEnd(Class<?> end) {
		this.end = end;
	}
	
	
	public void addIntermediate(Class<?> intermediate, long size) {
		intermediates.add(intermediate);
		intermediateSize.add(size);
	}

	public void updateIntermediate(Class<?> clazz, long bytes) {
		int idx = intermediates.indexOf(clazz);
		
		if (idx != -1) {
			Long tmp = intermediateSize.get(idx);
			tmp += bytes;
			intermediateSize.set(idx, tmp);
		}
	}
	
}