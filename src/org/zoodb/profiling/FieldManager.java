package org.zoodb.profiling;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tobiasg
 *
 */
public class FieldManager implements IFieldManager {
	
	
	/**
	 * String holds classname
	 * Map<String,ObjectFieldStats holds field accesses per object (String holds objectId)
	 */
	private Map<String,Map<String,ObjectFieldStats>> allClasses;
	
	public FieldManager() {
		allClasses = new HashMap<String,Map<String,ObjectFieldStats>>();
	}
	
	public void addAddFieldAccess(FieldAccess fa) {
		Map<String,ObjectFieldStats> classStats = allClasses.get(fa.getClassName());
		
		if (classStats == null) {
			classStats = new HashMap<String,ObjectFieldStats>();
		} 
		
		ObjectFieldStats objectStats = classStats.get(fa.getObjectId());
		
		if (objectStats == null) {
			ObjectFieldStats ofs = new ObjectFieldStats(fa.getClassName(), fa.getObjectId());
		}
		
	}

}
