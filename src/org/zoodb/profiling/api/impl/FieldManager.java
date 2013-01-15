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
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldSuggestion;


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
	
	private Map<Class<?>,LobCandidate> lobCandidates;
	
	
	private Map<String,Map<String,ObjectFieldStats>> allClasses;
	private Logger logger = ProfilingManager.getProfilingLogger();
	
	public FieldManager() {
		allClasses = new HashMap<String,Map<String,ObjectFieldStats>>();
		fieldAccesses = new HashMap<String,IFieldAccess>();
		lobCandidates = new HashMap<Class<?>,LobCandidate>();
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
	
	@Override
	public void updateLobCandidates(Class<?> clazz, Field f) {
		LobCandidate lc = lobCandidates.get(clazz);
		
		if (lc == null) {
			lc = new LobCandidate(clazz);
		}
		lc.incDetectionCount(f);
		lobCandidates.put(clazz, lc);
	}
	
	public Collection<AbstractSuggestion> getLOBSuggestions() {
		Collection<AbstractSuggestion> result = new LinkedList<AbstractSuggestion>();
		
		for (LobCandidate lc : lobCandidates.values()) {
			
			Collection<Field> fields = lc.getFields();
			
			for (Field f : fields) {
				int totalAccessCount = getCountByClassField(lc.getClazz(),f.getName(),null);
				int detectionCount = lc.getDetectionsByField(f);
				
				double ratio = detectionCount / (double) totalAccessCount;
				
				if (ratio > 1) {
					result.add(SuggestionFactory.getLS(lc.getClazz(), f, detectionCount, totalAccessCount));
				}
			}
		}
		
		return result;
	}
	
	private int getCountByClassField(Class<?> c,String fieldName,String trxId) {
		//until we have organized the fieldaccess in another way, we have to compute the count inefficiently
		int count = 0;
		for (IFieldAccess fa : fieldAccesses.values()) {
			if (fa.getAssocClass() ==c && fa.getFieldName().equals(fieldName)) {
				if (trxId == null) {
					count++;
				} else if (trxId.equals(fa.getUniqueTrxId())) {
					count++;
				}
			}
		}
		return count;
	}


	@Override
	public int get(Class c, String field, String trx) {
		return getCountByClassField(c,field,trx);
	}
	
	
	
	

}
