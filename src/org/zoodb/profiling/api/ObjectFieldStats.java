package org.zoodb.profiling.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.zoodb.profiling.api.impl.FieldStats;

/**
 * @author tobiasg
 *
 */
public class ObjectFieldStats {
	
	private String objectId;
	private IFieldStats fieldStats;
	
	public ObjectFieldStats(String clazzName, String objectId) {
		fieldStats = new FieldStats();
		this.objectId = objectId;
	}

	public String getObjectId() {
		return objectId;
	}

	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}
	
	public void addRead(String fieldName) {
		fieldStats.addFieldAccess(fieldName, true);
	}
	
	public void addWrite(String fieldName) {
		fieldStats.addFieldAccess(fieldName, false);
	}
	
	public Collection<String> getFieldsRead() {
		return fieldStats.getFieldsAccessed(true);
	}
	
	public void addFieldReadSize(String fieldName, long bytesCount) {
		fieldStats.addDeserialization(fieldName, bytesCount);
	}
	
	public long getBytesReadForField(String fieldName) {
		return fieldStats.getTotalDeserializationBytes(fieldName);
	}

}
