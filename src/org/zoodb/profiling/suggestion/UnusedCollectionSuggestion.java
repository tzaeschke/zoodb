package org.zoodb.profiling.suggestion;

/**
 * @author tobiasg
 *
 */
public class UnusedCollectionSuggestion extends FieldSuggestion {
	
	private final String identifier = "UNUSED_COLLECTION_SUGGESTION";
	
	private String triggerName;
	
	private long totalCollectionBytes;

	public String getTriggerName() {
		return triggerName;
	}

	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}

	public long getTotalCollectionBytes() {
		return totalCollectionBytes;
	}

	public void setTotalCollectionBytes(long totalCollectionBytes) {
		this.totalCollectionBytes = totalCollectionBytes;
	}
	
	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(identifier);
		sb.append(": ");
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(field.getName());
		sb.append(",Bytes(r)=");
		sb.append(totalCollectionBytes);
		sb.append(", never accessed");
		sb.append(", triggered by: ");
		sb.append(field.getName());
		sb.append('.');
		sb.append(triggerName);
		
		return sb.toString();
	}
	
	
}
