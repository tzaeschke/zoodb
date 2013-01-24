package org.zoodb.profiling.api.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClassSizeStats {
	
	private Map<String,Double> avgFieldSize;
	
	private Map<String,Integer> updateCounter;
	

	public ClassSizeStats() {
		avgFieldSize = new HashMap<String,Double>();
		updateCounter = new HashMap<String,Integer>();
	}
	
	public void updateField(String field, long byteCount) {
		Double d = avgFieldSize.get(field);
		
		if (d == null) {
			d = (double) byteCount;
			avgFieldSize.put(field, d);
			updateCounter.put(field, new Integer(1));
		} else {
			Integer updateCount = updateCounter.get(field);
			
			//calculate new avg
			Double total = (d*updateCount) + byteCount;
			
			updateCount++;
			updateCounter.put(field, updateCount);
			
			d = total / updateCount;
			avgFieldSize.put(field, d);
		}
	}
	
	public Double getAvgFieldSizeForField(String field) {
		return avgFieldSize.get(field);
	}
	
	public Set<String> getAllFields() {
		return avgFieldSize.keySet();
	}

}