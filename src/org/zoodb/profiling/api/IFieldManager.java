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

}
