package org.zoodb.profiling.api.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;

public class ClassSizeStats {
	
	private static class AvgValue {
		long sum;
		long div;
		double avg() {
			return sum/div;
		}
		public void update(long byteCount) {
			sum += byteCount;
			div++;
		}
	}
	
	private ZooClassDef def;
	private AvgValue[] averages;
	
	private final Map<String,Double> avgFieldSize;
	
	private final Map<String,Integer> updateCounter;
	
	private long totalClassSize;
	private int totalDeserializations;
	

	public ClassSizeStats(ZooClassDef def) {
		this.def = def;
		averages = new AvgValue[def.getAllFields().length];
		avgFieldSize = new HashMap<String,Double>();
		updateCounter = new HashMap<String,Integer>();
	}
	
	public void updateField(ZooFieldDef fDef, long byteCount) {
		//TODO remove this stuff
		String field = fDef.getName();
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
		
		AvgValue avg = averages[fDef.getFieldPos()];
		if (avg == null) {
			avg = new AvgValue();
			averages[fDef.getFieldPos()] = avg;
		}
		avg.update(byteCount);
	}
	
	public void updateClass(long bytes) {
		totalClassSize += bytes;
		totalDeserializations++;
	}
	
	public Double getAvgFieldSizeForField(String field) {
		return avgFieldSize.get(field);
	}
	public ZooFieldDef[] getAllFields() {
		return def.getAllFields();
	}
	public double getAvgClassSize() {
		return totalDeserializations > 0 ? totalClassSize/totalDeserializations : 0;
	}
	public int getTotalDeserializations() {
		return totalDeserializations;
	}
	public double[] getAvgFieldSizes() {
		double[] ret = new double[avgFieldSize.size()];
		for (int i = 0; i < averages.length; i++) {
			if (averages[i] != null) {
				ret[i] = averages[i].avg();
			}
		}
		return ret;
	}
}