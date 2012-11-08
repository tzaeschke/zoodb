package org.zoodb.profiling.suggestion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.ObjectFieldStats;


public class FieldSuggestion extends AbstractSuggestion {
	
	protected String fieldName;
	
	protected Map<String,ObjectFieldStats> clazzStats;
	
	private Collection<String> unusedFields;
	
	private Collection<IFieldAccess> clazzStats2;
	
	public FieldSuggestion(String fieldName) {
		this.fieldName = fieldName;
	}
	
	public FieldSuggestion() {
		this.unusedFields = new ArrayList<String>();
	}
	
	public void addUnusedFieldName(String fieldName) {
		this.unusedFields.add(fieldName);
	}
	
	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		int unusedFieldsCount = unusedFields.size();
		
		if (unusedFieldsCount == 1) {
			sb.append("The following field of class '");
			sb.append(this.getClazzName());
			sb.append(" has never been accessed: ");
			sb.append(unusedFields.iterator().next());
			sb.append(". Consider removing it.");
			return sb.toString();
		} else {
			sb.append("The following fields of class '");
			sb.append(this.getClazzName());
			sb.append(" have never been accessed: ");
			
			Iterator iter = unusedFields.iterator();
			
			while(iter.hasNext()) {
				sb.append(iter.next());
				sb.append(" ");
			}
			
			sb.append(". Consider outsourcing them in a separate class.");
			return sb.toString();
		}
	
	}
	
	public void setClazzStats(Map<String,ObjectFieldStats> clazzStats) {
		this.clazzStats = clazzStats;
	}
	
	public void setFieldAccesses(Collection<IFieldAccess> accesses) {
		this.clazzStats2 = accesses;
	}
	
	/**
	 * @returns total bytes read caused by this field (aggregated over all instances of the class) 
	 */
	public long getReadEffort() {
		long tmp = 0;
		for (String oid : clazzStats.keySet() ) {
			tmp += clazzStats.get(oid).getBytesReadForField(this.fieldName.toLowerCase());
		}
		return tmp;
	}
	
	public long getTotalEffort() {
		long tmp = 0;
		for (IFieldAccess fa : clazzStats2 ) {
			tmp += fa.sizeInBytes();
		}
		return tmp;
	}
}
