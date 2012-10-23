package org.zoodb.profiling.api;

import java.util.Collection;

/**
 * @author tobiasg
 *
 */
public interface IFieldManager {
	
	/**
	 * @param fa
	 */
	public void addAddFieldAccess(FieldAccess fa);
	
	/**
	 * 
	 */
	public void prettyPrintFieldAccess();
	
	/**
	 * @returns Suggestions based on field usage on class level
	 */
	public Collection<?> getFieldSuggestions();
	
	/**
	 * @param oid
	 * @param clazzName
	 * @param fieldName
	 * @param bytesCount
	 */
	public void addFieldRead(long oid, String clazzName, String fieldName, long bytesCount);

}
