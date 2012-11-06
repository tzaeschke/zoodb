package org.zoodb.profiling.api.impl;

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

}
