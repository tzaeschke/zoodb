package org.zoodb.profiling.suggestion;

public class FieldRemovalSuggestion extends FieldSuggestion {
	
	private final String identifier = "FIELD_REMOVAL_SUGGESTION";

	public FieldRemovalSuggestion(String fieldName) {
		super(fieldName);
	}
	
	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(identifier);
		sb.append(": ");
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(fieldName);
		sb.append(",Bytes(r)=");
		sb.append(getReadEffort());
		sb.append(", never accessed");
		
		return sb.toString();
	}
	
}