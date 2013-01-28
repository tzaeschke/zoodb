package org.zoodb.profiling.api.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;


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
	
	private Map<Class<?>,LobDetectionArchive> lobCandidates;
	
	
	private Logger logger = ProfilingManager.getProfilingLogger();
	
	public FieldManager() {
		fieldAccesses = new HashMap<String,IFieldAccess>();
		lobCandidates = new HashMap<Class<?>,LobDetectionArchive>();
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
		LobDetectionArchive lc = lobCandidates.get(clazz);
		
		if (lc == null) {
			lc = new LobDetectionArchive(clazz);
		}
		lc.incDetectionCount(f);
		lobCandidates.put(clazz, lc);
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
	public int get(Class<?> c, String field, String trx) {
		return getCountByClassField(c,field,trx);
	}
	
	@Override
	public Collection<LobDetectionArchive> getLOBCandidates() {
		return lobCandidates.values();
	}


	@Override
	public int[] getRWCount(Class<?> c, String fieldName) {
		int[] count = new int[2];
		
		for (IFieldAccess fa : fieldAccesses.values()) {
			if (fa.getAssocClass() ==c && fa.getFieldName().equals(fieldName) && fa.isActive()) {
				if (!fa.isWrite()) {
					count[0]++;
				} else {
					count[1]++;
				}
			}
		}
		return count;
	}


	@Override
	public int getWriteCount(Class<?> c) {
		Set<String> writes = new HashSet<String>();
		
		//count distinct (oid,trx) pairs
		for (IFieldAccess fa : fieldAccesses.values()) {
			if (fa.isActive() && fa.getAssocClass() == c && fa.isWrite()) {
				writes.add(fa.getOid() + fa.getUniqueTrxId());
			}
		}
		
		return writes.size();
	}
	

}