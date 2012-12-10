package org.zoodb.profiling.suggestion;

public class FieldRemovalSuggestion extends FieldSuggestion {
	
	private final String identifier = "FIELD_UNUSED";


	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Class ");
		sb.append(getClazzName());
		sb.append(" has unused field: ");
		sb.append(getFieldName());
		
		return sb.toString();
	}
	
	public String getIdentifier() {
		return identifier;
	}
	
	
	
}