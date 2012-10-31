package org.zoodb.profiling.api;

import java.util.Collection;

public interface IFieldStats {
	
	public void addDeserialization(String fieldName, long bytes);
	
	public void addSerialization(String fieldName, long bytes);
	
	
	public int getDeserializationCount(String fieldName);
	
	public int getSerializationCount(String fieldName);
	
	
	public long getTotalDeserializationBytes(String fieldName);
	
	public long getTotalSerializationBytes(String fieldName);
	
	
	public Collection<String> getDeserializedFields();
	
	public Collection<String> getSerializedFields();
	
	
	/**
	 * Is not always exact: estimated via method-name which triggered activation
	 * @param fieldName
	 * @param read
	 */
	public void addFieldAccess(String fieldName, boolean read);
	
	public int getAccessCountByField(String fieldName);
	
	public Collection<String> getFieldsAccessed(boolean read);
	

}
