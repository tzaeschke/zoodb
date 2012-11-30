package org.zoodb.profiling.suggestion;

public class CollectionAccessRefSuggestion extends FieldSuggestion {
	
	private final String identifier = "REF_DIRECT";
	
	private String itemClazzName;

	public String getItemClazzName() {
		return itemClazzName;
	}

	public void setItemClazzName(String itemClazzName) {
		this.itemClazzName = itemClazzName;
	}
	
	

}
