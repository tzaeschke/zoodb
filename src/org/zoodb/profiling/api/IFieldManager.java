package org.zoodb.profiling.api;

import java.lang.reflect.Field;
import java.util.Collection;

import org.zoodb.profiling.api.impl.LobDetectionArchive;

/**
 * @author tobiasg
 *
 */
public interface IFieldManager {
	

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