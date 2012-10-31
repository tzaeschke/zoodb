package org.zoodb.profiling.api.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.FieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.ObjectFieldStats;
import org.zoodb.profiling.suggestion.FieldSuggestion;

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
	private Logger logger = LogManager.getLogger("allLogger");
	
	public FieldManager() {
		allClasses = new HashMap<String,Map<String,ObjectFieldStats>>();
	}
	
	@Override
	public void addAddFieldAccess(FieldAccess fa) {
		Map<String,ObjectFieldStats> classStats = allClasses.get(fa.getClassName());
		
		if (classStats == null) {
			classStats = new HashMap<String,ObjectFieldStats>();
		} 
		
		ObjectFieldStats objectStats = classStats.get(fa.getObjectId());
		
		if (objectStats == null) {
			objectStats = new ObjectFieldStats(fa.getObjectId());
		}
		
		objectStats.addRead(fa.getFieldName());
		classStats.put(fa.getObjectId(), objectStats);
		allClasses.put(fa.getClassName(), classStats);
		
	}

	@Override
	public void prettyPrintFieldAccess() {
		for (String clazzName : allClasses.keySet()) {
			logger.info("Printing field access for class: " + clazzName);
			Map<String,ObjectFieldStats> classStats = allClasses.get(clazzName);
			
			for (String object : classStats.keySet() ) {
				ObjectFieldStats ofs = classStats.get(object);
				logger.info("\t Printing field access for object: " + object);
				for (String fieldName : ofs.getFieldsRead()) {
					logger.info("\t\t" + fieldName + " bytes:" + ofs.getBytesReadForField(fieldName));
				}
			}
		}
		
	}

	@Override
	public Collection<FieldSuggestion> getFieldSuggestions() {
		Collection<FieldSuggestion> suggestions = new ArrayList<FieldSuggestion>();
		
		Set<String> clazzNames = allClasses.keySet();
		for (String clazzName : clazzNames) {
			suggestions.addAll(getUnaccessedFieldsByClassSuggestion(clazzName));
		}
		
		
		return suggestions;
	}
	
	/**
	 * Reports fields of class 'clazzName' that are never accessed (neither read nor write)
	 * @param clazzName
	 * @return
	 */
	private Collection<? extends FieldSuggestion> getUnaccessedFieldsByClassSuggestion(String clazzName) {
		Collection<FieldSuggestion> suggestionsByClass = new LinkedList<FieldSuggestion>();
		
		Collection<String> fieldsUsed = getFieldsUsedByClass(clazzName);
		FieldSuggestion  fs = null;
		
		try {
			//getDeclaredFields() will not return inherited fields.
			//TODO: find a way to detect unused inherited fields.
			Field[] fieldsDeclared = Class.forName(clazzName).getDeclaredFields();
			
			//fieldsDeclared is an array, cannot use 'retainAll'...
			for (Field f : fieldsDeclared) {
				if ( !fieldsUsed.contains(f.getName().toLowerCase()) ) {
					if (fs == null) {
						fs = new FieldSuggestion();
						fs.setClazzName(clazzName);
					}
					fs.addUnusedFieldName(f.getName()) ;
				}
			}
			if (fs != null) {
				logger.info(fs.getText());
				suggestionsByClass.add(fs);
			}
			
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		return suggestionsByClass;
	}
	
	/**
	 * Reports fields accessed by any object of class 'clazzName'
	 * @param clazzName
	 * @return
	 */
	private Collection<String> getFieldsUsedByClass(String clazzName) {
		Map<String,ObjectFieldStats> allObjects = allClasses.get(clazzName);
		
		Collection<String> usedFields = new HashSet<String>();
		
		for (ObjectFieldStats ofs : allObjects.values()) {
			usedFields.addAll(ofs.getFieldsRead());
		}

		return usedFields;
	}

	@Override
	public void addFieldRead(long oid, String clazzName, String fieldName, long bytesCount) {
		Map<String,ObjectFieldStats> classStats = allClasses.get(clazzName);
		
		if (classStats == null) {
			classStats = new HashMap<String,ObjectFieldStats>();
		} 
		
		ObjectFieldStats ofs = classStats.get(String.valueOf(oid));
		
		if (ofs == null) {
			ofs = new ObjectFieldStats(String.valueOf(oid));
		}
		
		ofs.addFieldReadSize(fieldName.toLowerCase(), bytesCount);
		
		classStats.put(String.valueOf(oid), ofs);
		allClasses.put(clazzName, classStats);
	
	}

}
