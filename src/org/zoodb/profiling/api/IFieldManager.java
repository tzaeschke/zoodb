package org.zoodb.profiling.api;

import java.lang.reflect.Field;
import java.util.Collection;

import org.zoodb.profiling.api.impl.LobCandidate;

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
	
	
	
	public Collection<IFieldAccess> get(long oid, String trx);
	
	/**
	 * Returns the number of fieldAccesses on 'c.field' in transaction 'trx'
	 * @param c
	 * @param trx
	 * @return
	 */
	public int get(Class<?> c, String field, String trx);
	
	public void updateLobCandidates(Class<?> clazz, Field f);
	
	public Collection<LobCandidate> getLOBCandidates();
	
}