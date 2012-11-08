package org.zoodb.profiling.api;

import java.util.Set;

/**
 * Abstraction of collected profiling data
 * @author tobiasg
 *
 */
public interface IDataProvider {
	
	/**
	 * Distinct set of classes for which field accesses are available
	 * @return
	 */
	public Set<Class<?>> getClasses();
	
	/**
	 * Set of field accesses of class 'c' (over all trx)
	 * @param c
	 * @return
	 */
	public Set<IFieldAccess> getByClass(Class<?> c);
	
	
	/**
	 * Set of field accesses for class 'c' and field 'fieldName'
	 * @param c
	 * @param fieldName
	 * @return
	 */
	public Set<IFieldAccess> getByClassAndField(Class<?> c, String fieldName);
	
}
