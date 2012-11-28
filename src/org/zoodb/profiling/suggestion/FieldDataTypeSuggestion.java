package org.zoodb.profiling.suggestion;

public class FieldDataTypeSuggestion extends FieldSuggestion {
	
	private final String identifier = "DATA_TYPE_SUGGESTION";
	
	private Class<?> suggestedType;
	private Class<?> currentType;
	

	
	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(identifier);
		sb.append(": ");
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(getFieldName());
		sb.append(",currentType=");
		sb.append(currentType.getName());
		//sb.append(",Bytes(r)=");
		//sb.append(getTotalEffort());
		sb.append(",new Type=");
		sb.append(suggestedType.getName());
		
		return sb.toString();
	}
	
	
	public void setSuggestedType(Class<?> suggestedType) {
		this.suggestedType = suggestedType;
	}
	
	public Class<?> getSuggestedType() {
		return this.suggestedType;
	}

	public Class<?> getCurrentType() {
		return currentType;
	}

	public void setCurrentType(Class<?> currentType) {
		this.currentType = currentType;
	}
	
}