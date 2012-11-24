package org.zoodb.profiling.suggestion;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.ObjectFieldStats;


public class FieldSuggestion extends AbstractSuggestion {
	
	protected Field field;
	
	protected String fieldName;
	
	private transient Collection<IFieldAccess> clazzStats2;
	
	public FieldSuggestion(String fieldName) {
		this.fieldName = fieldName;
	}
	
	public FieldSuggestion() {
		
	}
	
	public void setFieldAccesses(Collection<IFieldAccess> accesses) {
		this.clazzStats2 = accesses;
	}
	
	/**
	 * @returns total bytes read caused by this field (aggregated over all instances of the class) 
	 */
	public long getTotalEffort() {
		long tmp = 0;
		for (IFieldAccess fa : clazzStats2 ) {
			tmp += fa.sizeInBytes();
		}
		return tmp;
	}

	public Field getField() {
		return field;
	}

	public void setField(Field field) {
		this.field = field;
	}

	@Override
	public void apply(Object model) {
		// TODO Auto-generated method stub
		
	}
	
	
}
