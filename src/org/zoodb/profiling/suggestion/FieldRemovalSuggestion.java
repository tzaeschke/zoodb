package org.zoodb.profiling.suggestion;

public class FieldRemovalSuggestion extends FieldSuggestion {
	
	private final String identifier = "FIELD_REMOVAL_SUGGESTION";


	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(identifier);
		sb.append(": ");
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(getFieldName());
		//sb.append(",Bytes(r)=");
		//sb.append(getTotalEffort());
		
		sb.append(", never accessed");
		
		return sb.toString();
	}
	
}