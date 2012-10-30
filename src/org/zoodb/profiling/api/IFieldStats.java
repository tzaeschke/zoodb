package org.zoodb.profiling.api;

import java.util.Collection;

public interface IFieldStats {
	
	public void addDeserialization(String fieldName, long bytes);
	
	public void addSeriealization(String fieldName, long bytes);
	
	
	public int getDeserializationCount(String fieldName);
	
	public int getSerializationCount(String fieldName);
	
	
	public long getTotalDeserializationBytes(String fieldName);
	
	public long getTotalSerializationBytes(String fieldName);
	
	
	public Collection<String> getDeserializedFields();
	
	public Collection<String> getSerializedFields();
	

}
