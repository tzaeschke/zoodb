package org.zoodb.profiling.suggestion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.zoodb.profiling.api.impl.AbstractSuggestion;

public class FieldSuggestion extends AbstractSuggestion {
	
	private Class clazz;
	
	private Collection<String> unusedFields;
	
	public FieldSuggestion(Class clazz) {
		this.clazz = clazz;
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
			sb.append(clazz.getName());
			sb.append(" has never been accessed: ");
			sb.append(unusedFields.iterator().next());
			sb.append(". Consider removing it.");
			return sb.toString();
		} else {
			sb.append("The following fields of class '");
			sb.append(clazz.getName());
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
}
