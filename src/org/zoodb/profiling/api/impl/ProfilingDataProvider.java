package org.zoodb.profiling.api.impl;

import java.util.HashSet;
import java.util.Iterator;
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
	
	private FieldManager fieldManager;

	
	
	public IFieldManager getFieldManager() {
		return fieldManager;
	}

	public void setFieldManager(FieldManager fieldManager) {
		this.fieldManager = fieldManager;
	}

	@Override
	public Set<Class<?>> getClasses() {
		Iterator<IFieldAccess> iter = fieldManager.getFieldAccesses().values().iterator();
		
		Set<Class<?>> distinctClasses = new HashSet<Class<?>>();
		
		while (iter.hasNext()) {
			distinctClasses.add(iter.next().getAssocClass());
		}
		return distinctClasses;
	}

	@Override
	public Set<IFieldAccess> getByClass(Class<?> c) {
		Set<IFieldAccess> byClass = new HashSet<IFieldAccess>();
		
		Iterator<IFieldAccess> iter = fieldManager.getFieldAccesses().values().iterator();
		
		while (iter.hasNext()) {
			IFieldAccess candidate = iter.next();
			if (candidate.getAssocClass() == c) {
				byClass.add(candidate);
			}
		}
		
		return byClass;
	}

	@Override
	public Set<IFieldAccess> getByClassAndField(Class<?> c, String fieldName) {
		Set<IFieldAccess> byClassAndField = new HashSet<IFieldAccess>();
		
		Iterator<IFieldAccess> iter = fieldManager.getFieldAccesses().values().iterator();
		
		while (iter.hasNext()) {
			IFieldAccess candidate = iter.next();
			if (candidate.getAssocClass() == c && candidate.getFieldName().equals(fieldName)) {
				byClassAndField.add(candidate);
			}
		}
		
		return byClassAndField;
	}

}
