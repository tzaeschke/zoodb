package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.zoodb.profiling.api.IFieldStats;

public class FieldStats implements IFieldStats {
	
	private Map<String,Integer> writeCounter;
	private Map<String,Integer> readCounter;
	private Map<String,Long> deserializations;
	
	private Map<String,Integer> deserializationCounter;
	
	public FieldStats() {
		writeCounter = new HashMap<String,Integer>();
		readCounter = new HashMap<String,Integer>();
		
		deserializations = new HashMap<String,Long>();
		deserializationCounter = new HashMap<String,Integer>();
	}
	

	@Override
	public void addDeserialization(String fieldName, long bytes) {
		Long tmp = deserializations.get(fieldName);
		tmp = tmp != null ? tmp+bytes : bytes;
		deserializations.put(fieldName, tmp);
		
		Integer c = deserializationCounter.get(fieldName);
		c = c == null? 1 : c++;
		deserializationCounter.put(fieldName, c);
	}

	@Override
	public void addSerialization(String fieldName, long bytes) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getDeserializationCount(String fieldName) {
		Integer value = deserializationCounter.get(fieldName);
		return value == null? 0 : value;
	}

	@Override
	public int getSerializationCount(String fieldName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getTotalDeserializationBytes(String fieldName) {
		Long bytesCount = deserializations.get(fieldName);
		return bytesCount != null ? bytesCount : 0;
	}

	@Override
	public long getTotalSerializationBytes(String fieldName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Collection<String> getDeserializedFields() {
		return deserializations.keySet();
	}

	@Override
	public Collection<String> getSerializedFields() {
		return null;
	}

	@Override
	public void addFieldAccess(String fieldName, boolean read) {
		if (read) {
			Integer value = readCounter.get(fieldName);
			value = value == null? 1 : value++;
			readCounter.put(fieldName, value);
		} else {
			
		}
	}

	@Override
	public int getAccessCountByField(String fieldName) {
		Integer value = readCounter.get(fieldName);
		return value == null? 0 : value;
	}

	@Override
	public Collection<String> getFieldsAccessed(boolean read) {
		return read ? readCounter.keySet() : null;
	}

}
