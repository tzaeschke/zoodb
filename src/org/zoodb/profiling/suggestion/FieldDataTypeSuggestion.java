package org.zoodb.profiling.suggestion;

public class FieldDataTypeSuggestion extends FieldSuggestion {
	
	private final String identifier = "DATA_TYPE_SUGGESTION";
	
	private String suggestedType;
	private String currentType;
	

	
	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(getFieldName());
		sb.append(",currentType=");
		sb.append(currentType);
		//sb.append(",Bytes(r)=");
		//sb.append(getTotalEffort());
		sb.append(",new Type=");
		sb.append(suggestedType);
		
		return sb.toString();
	}
	
	
	public void setSuggestedType(String suggestedType) {
		this.suggestedType = suggestedType;
	}
	
	public String getSuggestedType() {
		return this.suggestedType;
	}

	public String getCurrentType() {
		return currentType;
	}

	public void setCurrentType(String currentType) {
		this.currentType = currentType;
	}
	
}