package org.zoodb.profiling.analyzer;

import java.util.LinkedList;
import java.util.List;

public class ShortcutCandidate {
	
	private Class<?> start;
	private Class<?> end;
	
	private List<Class<?>> intermediates;
	private List<Long> intermediateSize;
	private List<Integer> intermediateVisitCounter;
	

	public ShortcutCandidate() {
		intermediates = new LinkedList<Class<?>>();
		intermediateSize = new LinkedList<Long>();
		intermediateVisitCounter = new LinkedList<Integer>();
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
	
	public List<Integer> getIntermediateVisitCounter() {
		return intermediateVisitCounter;
	}

	public void setIntermediateVisitCounter(List<Integer> intermediateVisitCounter) {
		this.intermediateVisitCounter = intermediateVisitCounter;
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
		intermediateVisitCounter.add(1);
	}

	public void updateIntermediate(Class<?> clazz, long bytes) {
		int idx = intermediates.indexOf(clazz);
		
		if (idx != -1) {
			//update bytes
			Long tmp = intermediateSize.get(idx);
			tmp += bytes;
			intermediateSize.set(idx, tmp);
			
			//update visitCounter
			int tmpVisitCounter = intermediateVisitCounter.get(idx);
			tmpVisitCounter++;
			intermediateVisitCounter.set(idx, tmpVisitCounter);
		}
	}

	public boolean samePath(Class<?> start2, Class<?> end2, List<Class<?>> intermediate, List<Long> intermediateSize2) {
		boolean same = true;
		
		same &= start2 == this.start;
		same &= end2 == this.end;
		same &= intermediate.size() == this.intermediates.size();
		
		if (same) {
			//check if all intermediate nodes are equal
			int size = intermediates.size();
			for (int i=0;i<size;i++) {
				same &= this.intermediates.get(i) == intermediate.get(i);
			}
		}
		
		return same;
	}

	public void update(List<Long> intermediateSize2) {
		int count = intermediateVisitCounter.size();
		for (int i=0;i<count;i++) {
			//update visit counters
			int tmp = intermediateVisitCounter.get(i);
			tmp++;
			intermediateVisitCounter.set(i, tmp);
		}
		for (int i=0;i<intermediates.size();i++) {
			Long l = intermediateSize.get(i);
			l += intermediateSize2.get(i);
			intermediateSize.set(i, l);
		}
		
	}
	
}