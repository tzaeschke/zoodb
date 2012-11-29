package org.zoodb.profiling.suggestion;

public class FieldRemovalSuggestion extends FieldSuggestion {
	
	private final String identifier = "FIELD_REMOVAL";


	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(getFieldName());
		//sb.append(",Bytes(r)=");
		//sb.append(getTotalEffort());
		
		sb.append(", never accessed");
		
		return sb.toString();
	}
	
	public String provideLabelForColumn(int columnIndex) {
		switch(columnIndex) {
			case 0:
				return getText();
			case 1:
				return getClazzName();
			case 2:
				return identifier;
			default:
				return null;
		}

	}
	
}