package org.zoodb.profiling.api.impl;

import java.util.HashMap;
import java.util.Map;

import org.zoodb.profiling.api.FieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.ObjectFieldStats;

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
			objectStats = new ObjectFieldStats(fa.getClassName(), fa.getObjectId());
		}
		
		objectStats.addRead(fa.getFieldName());
		classStats.put(fa.getObjectId(), objectStats);
		allClasses.put(fa.getClassName(), classStats);
		
	}

	@Override
	public void prettyPrintFieldAccess() {
		for (String clazzName : allClasses.keySet()) {
			System.out.println("Printing field access for class: " + clazzName);
			Map<String,ObjectFieldStats> classStats = allClasses.get(clazzName);
			
			for (String object : classStats.keySet() ) {
				ObjectFieldStats ofs = classStats.get(object);
				System.out.println("\t Printing field access for object: " + object);
				for (String fieldName : ofs.getFieldsRead()) {
					System.out.println("\t\t" + fieldName);
				}
			}
		}
		
	}

}
