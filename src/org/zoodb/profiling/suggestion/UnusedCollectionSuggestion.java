package org.zoodb.profiling.suggestion;

/**
 * @author tobiasg
 *
 */
public class UnusedCollectionSuggestion extends CollectionSuggestion {
	
	private final String identifier = "UNUSED_COLLECTION_SUGGESTION";
	
	
	
	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(identifier);
		sb.append(": ");
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(getFieldName());
		sb.append(",Bytes(r)=");
		sb.append(getTotalCollectionBytes());
		sb.append(", never accessed");
		sb.append(", triggered by: ");
		sb.append(getFieldName());
		sb.append('.');
		sb.append(getTriggerName());
		
		return sb.toString();
	}
	
	public String getIdentifier() {
		return identifier;
	}
	
	
}
