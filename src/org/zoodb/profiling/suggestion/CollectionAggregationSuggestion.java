package org.zoodb.profiling.suggestion;

public class CollectionAggregationSuggestion extends CollectionSuggestion {
	
	private final String identifier = "COLLECTION_AGGREGATION_SUGGESTION";
	
	private Class<?> collectionItem;
	
	public Class<?> getCollectionItem() {
		return collectionItem;
	}

	public void setCollectionItem(Class<?> collectionItem) {
		this.collectionItem = collectionItem;
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
		sb.append(getTotalCollectionBytes());
		sb.append(", all accessed");
		sb.append(", triggered by: ");
		sb.append(field.getName());
		sb.append('.');
		sb.append(getTriggerName());
		
		return sb.toString();
	}
	

}
