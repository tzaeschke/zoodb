package org.zoodb.profiling.api.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.ObjectFieldStats;
import org.zoodb.profiling.suggestion.FieldRemovalSuggestion;
import org.zoodb.profiling.suggestion.FieldSuggestion;
import org.zoodb.profiling.suggestion.FieldDataTypeSuggestion;

/**
 * @author tobiasg
 *
 */
public class FieldManager implements IFieldManager {
	
	
	/**
	 * String holds classname
	 * Map<String,ObjectFieldStats holds field accesses per object (String holds objectId)
	 */
	private Map<String,IFieldAccess> fieldAccesses;
	
	
	private Map<String,Map<String,ObjectFieldStats>> allClasses;
	private Logger logger = LogManager.getLogger("allLogger");
	
	public FieldManager() {
		allClasses = new HashMap<String,Map<String,ObjectFieldStats>>();
		fieldAccesses = new HashMap<String,IFieldAccess>();
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
//		Collection<FieldSuggestion> suggestions = new ArrayList<FieldSuggestion>();
//		
//		Set<String> clazzNames = allClasses.keySet();
//		for (String clazzName : clazzNames) {
//			suggestions.addAll(getUnaccessedFieldsByClassSuggestion(clazzName));
//			
//			suggestions.addAll(getDataTypeSuggestions(clazzName));
//		}
//		
//		
		return null;
	}
	
	/**
	 * Reports fields of class 'clazzName' that are never accessed (neither read nor write)
	 * @param clazzName
	 * @return
	 */
//	private Collection<? extends FieldSuggestion> getUnaccessedFieldsByClassSuggestion(String clazzName) {
//		Collection<FieldSuggestion> suggestionsByClass = new LinkedList<FieldSuggestion>();
//		
//		Collection<String> fieldsUsed = getFieldsUsedByClass(clazzName);
//		FieldSuggestion  fs = null;
//		
//		try {
//			//getDeclaredFields() will not return inherited fields.
//			//TODO: find a way to detect unused inherited fields.
//			Class<?> clazz = Class.forName(clazzName);
//			Field[] fieldsDeclared = clazz.getDeclaredFields();
//			
//			//fieldsDeclared is an array, cannot use 'retainAll'...
//			for (Field f : fieldsDeclared) {
//				if ( !fieldsUsed.contains(f.getName().toLowerCase()) ) {
//					fs = new FieldRemovalSuggestion(f.getName());
//					fs.setClazz(clazz);
//					//fs.setClazzStats(allClasses.get(clazzName));
//					logger.info(fs.getText());
//					suggestionsByClass.add(fs);
//				}
//			}
//			
//		} catch (SecurityException e) {
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} 
//		return suggestionsByClass;
//	}
	
	/**
	 * Reports fields accessed by any object of class 'clazzName'
	 * @param clazzName
	 * @return
	 */
//	private Collection<String> getFieldsUsedByClass(String clazzName) {
//		Map<String,ObjectFieldStats> allObjects = allClasses.get(clazzName);
//		
//		Collection<String> usedFields = new HashSet<String>();
//		
//		for (ObjectFieldStats ofs : allObjects.values()) {
//			usedFields.addAll(ofs.getFieldsRead());
//		}
//
//		return usedFields;
//	}
	
	
	/**
	 * Check the transient and collection fields of the class.
	 * 
	 *  If their access frequency is high and (de-)serialization effort is high, suggest to use ZooDB-collections
	 *  
	 * @param clazzName
	 * @return
	 */
//	private Collection<? extends FieldSuggestion> getDataTypeSuggestions(String clazzName) {
//		Map<String,ObjectFieldStats> allObjects = allClasses.get(clazzName);
//		Collection<FieldSuggestion> suggestions = new LinkedList<FieldSuggestion>();
//		
//		try {
//			Class<?> clazz = Class.forName(clazzName); 
//			Field[] fields = clazz.getDeclaredFields();
//			
//			
//			for (Field field : fields) {
//				if ( isNonTransientCollection(field) ) {
//
//					FieldDataTypeSuggestion fdts = new FieldDataTypeSuggestion(field.getName());
//					fdts.setClazz(clazz);
//					fdts.setCurrentType(field.getType());
//					fdts.setSuggestedType(Class.forName("org.zoodb.jdo.api.DBCollection"));
//					//fdts.setClazzStats(allObjects);
//					logger.info(fdts.getText());
//					
//					//get total deserialization/serialization effort for this field
//				}
//			}
//			
//		} catch (SecurityException e) {
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//		
//		return suggestions;
//	}
	

	
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
	
	/**
	 * @return true if 'field' is not transient and of a collection type except DBCollection
	 */
	private boolean isNonTransientCollection(Field field) {
		try {
			boolean nonTransient = !(field.getModifiers() == Modifier.TRANSIENT);
			boolean isCollection = Class.forName("java.util.Collection").isAssignableFrom(field.getType()) || field.getType().isArray(); 
			if ( nonTransient && isCollection) {
				if (Class.forName("org.zoodb.jdo.api.DBCollection").isAssignableFrom(field.getType())) {
					return false;
				} else {
					logger.info("collection attribute to optimize: " + field.getName() + " type:" + field.getType().getName()); 
					return true;
				}
			} else {
				return false;
			}
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	@Override
	public void insertFieldAccess(IFieldAccess fa) {
		//check if this access is already marked
		IFieldAccess originalAccess = fieldAccesses.get(fa.toString());
		
		if (originalAccess == null) {
			fieldAccesses.put(fa.toString(), fa);
		} else {
			originalAccess.setActive(true);
		}
		
	}

	public Map<String, IFieldAccess> getFieldAccesses() {
		return fieldAccesses;
	}

	@Override
	public Collection<IFieldAccess> get(long oid, String trx) {
		Collection<IFieldAccess> result = new LinkedList<IFieldAccess>();
		for (IFieldAccess fa : fieldAccesses.values()) {
			if (fa.getOid() == oid && trx.equals(fa.getUniqueTrxId()) && fa.isActive()) {
				result.add(fa);
			}
		}
		return result;
	}
	
	
	
	

}
