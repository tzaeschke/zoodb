package org.zoodb.profiling.suggestion;

public class CollectionSuggestion extends FieldSuggestion {
	
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

}
