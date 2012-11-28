package org.zoodb.profiling.suggestion;

import java.lang.reflect.Field;

public class CollectionAggregationSuggestion extends CollectionSuggestion {
	
	private final String identifier = "COLLECTION_AGGREGATION_SUGGESTION";
	
	/**
	 * Typename of collection items
	 * Will not be serialized --> serialization would require the class to be present (if we import these suggestions in AgileIS, this class will not be on the classpath)
	 * --> use typeName instead 
	 */
	private String collectionItemTypeName;
	
	/**
	 * Name of field in class that owns the collection (over which the aggregation took place)
	 * Field in owner of collection which holds collection
	 * Will not be serialized (same reason as above, field has reference to class object which is most likely not present upon deserialization)
	 */
	private String ownerCollectionFieldName;
	
	
	
	public String getCollectionItemTypeName() {
		return collectionItemTypeName;
	}

	public void setCollectionItemTypeName(String collectionItemTypeName) {
		this.collectionItemTypeName = collectionItemTypeName;
	}


	public String getOwnerCollectionFieldName() {
		return ownerCollectionFieldName;
	}

	public void setOwnerCollectionFieldName(String ownerCollectionFieldName) {
		this.ownerCollectionFieldName = ownerCollectionFieldName;
	}


	public String getIdentifier() {
		return identifier;
	}



	public String getText() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(identifier);
		sb.append(": ");
		sb.append("Class=");
		sb.append(getClazzName());
		sb.append(",Field=");
		sb.append(ownerCollectionFieldName);
		sb.append(",Bytes(r)=");
		sb.append(getTotalCollectionBytes());
		sb.append(", all accessed with single same child --> aggregation over ");
		sb.append(collectionItemTypeName);
		sb.append('.');
		sb.append(getFieldName());
		sb.append('?');
		
		return sb.toString();
	}
	

}