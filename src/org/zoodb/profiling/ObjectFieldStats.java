package org.zoodb.profiling;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tobiasg
 *
 */
public class ObjectFieldStats {
	
	private String clazzName;
	private String objectId;
	private Map<String,Integer> writes;
	private Map<String,Integer> reads;
	
	public ObjectFieldStats(String clazzName, String objectId) {
		this.clazzName = clazzName;
		this.objectId = objectId;
		
		writes = new HashMap<String,Integer>();
		reads = new HashMap<String,Integer>();
	}

	public String getClazzName() {
		return clazzName;
	}

	public void setClazzName(String clazzName) {
		this.clazzName = clazzName;
	}

	public String getObjectId() {
		return objectId;
	}

	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}
	
	public void addRead(String fieldName) {
		Integer value = writes.get(fieldName);
		value = value == null? 1 : value++;
		reads.put(fieldName, value);
	}
	
	public void addWrite(String fieldName) {
		Integer value = writes.get(fieldName);
		value++;
		writes.put(fieldName, value);
	}
	
	public Collection<String> getFieldsRead() {
		return reads.keySet();
	}

}
