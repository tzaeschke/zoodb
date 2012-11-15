package org.zoodb.profiling.suggestion;

import java.lang.reflect.Field;

public class CollectionAggregationSuggestion extends CollectionSuggestion {
	
	private final String identifier = "COLLECTION_AGGREGATION_SUGGESTION";
	
	/**
	 * Type of collection items 
	 */
	private Class<?> collectionItem;
	
	/**
	 * Field in owner of collection which holds collection 
	 */
	private Field ownerCollectionField;
	
	public Class<?> getCollectionItem() {
		return collectionItem;
	}

	public void setCollectionItem(Class<?> collectionItem) {
		this.collectionItem = collectionItem;
	}


	public Field getOwnerCollectionField() {
		return ownerCollectionField;
	}

	public void setOwnerCollectionField(Field ownerCollectionField) {
		this.ownerCollectionField = ownerCollectionField;
	}

	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(identifier);
		sb.append(": ");
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(ownerCollectionField.getName());
		sb.append(",Bytes(r)=");
		sb.append(getTotalCollectionBytes());
		sb.append(", all accessed with single same child --> aggregation over ");
		sb.append(collectionItem.getName());
		sb.append('.');
		sb.append(field.getName());
		sb.append('?');
//		sb.append(", triggered by: ");
//		sb.append(field.getName());
//		sb.append('.');
//		sb.append(getTriggerName());
		
		return sb.toString();
	}
	

}
