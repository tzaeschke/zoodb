package org.zoodb.profiling.api.impl;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LobDetectionArchive {
	
	private Class<?> clazz;
	
	private Map<Field,Integer> detectionCountByField;
	
	public LobDetectionArchive(Class<?> clazz) {
		this.clazz = clazz;
		detectionCountByField = new HashMap<Field,Integer>();
	}
	
	public Class<?> getClazz() {
		return clazz;
	}

	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}
	
	public void incDetectionCount(Field f) {
		Integer i = detectionCountByField.get(f);
		
		if (i == null) {
			i = new Integer(0);
		}
		i++;
		detectionCountByField.put(f, i);
	}
	
	public Collection<Field> getFields() {
		return detectionCountByField.keySet();
	}
	
	public int getDetectionsByField(Field f) {
		return detectionCountByField.get(f) == null ? 0 : detectionCountByField.get(f); 
	}

}