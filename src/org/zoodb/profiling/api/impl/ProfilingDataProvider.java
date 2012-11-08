package org.zoodb.profiling.api.impl;

import java.util.Set;

import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.IDataProvider;

/**
 * @author tobiasg
 * For now, simulate the storage via a field-manager
 * TODO: extract profiling data from storage (db4o?)
 */
public class ProfilingDataProvider implements IDataProvider {
	
	private IFieldManager fieldManager;

	
	
	public IFieldManager getFieldManager() {
		return fieldManager;
	}

	public void setFieldManager(IFieldManager fieldManager) {
		this.fieldManager = fieldManager;
	}

	@Override
	public Set<Class<?>> getClasses() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<IFieldAccess> getByClass(Class<?> c) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<IFieldAccess> getByClassAndField(Class<?> c, String fieldName) {
		// TODO Auto-generated method stub
		return null;
	}

}
