package org.zoodb.profiling.api;

import java.lang.reflect.Field;
import java.util.Collection;

import org.zoodb.profiling.api.impl.LobDetectionArchive;
import org.zoodb.profiling.api.impl.SimpleFieldAccess;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

/**
 * @author tobiasg
 *
 */
public interface IFieldManager {
	
	/**
	 * Archives a field access in the IFieldManagers registry
	 * 
	 * @param fa
	 */
	public void insertFieldAccess(IFieldAccess fa);
	
	
	
	//public Collection<IFieldAccess> get(long oid, String trx);
	
	/**
	 * Returns all field accesses for a given activation.
	 * TODO: replace all calls to get(oid,trx) with this method.
	 * @param a
	 * @return
	 */
	public Collection<SimpleFieldAccess> get(AbstractActivation a);
	
	/**
	 * Returns the number of fieldAccesses on 'c.field' in transaction 'trx'
	 * @param c
	 * @param trx
	 * @return
	 */
	public int get(Class<?> c, String field, String trx);
	
	public void updateLobCandidates(Class<?> clazz, Field f);
	
	public Collection<LobDetectionArchive> getLOBCandidates();
	
	/**
	 * Returns an array of size 2. 
	 * The first entry is the number of reads on c.fieldName
	 * The second entry is the number of writes on c.fieldName
	 * @param c
	 * @param fieldName
	 * @return
	 */
	public int[] getRWCount(Class<?> c, String fieldName);
	
	/**
	 * Returns the total number of writes on objects of class 'c'
	 * @return
	 */
	public int getWriteCount(Class<?> c);
	
}