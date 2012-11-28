package org.zoodb.profiling.suggestion;

public class SuggestionFactory {
	
	
	/**
	 * @param o
	 * @return
	 */
	public static CollectionAggregationSuggestion getCAS(Object[] o) {
		CollectionAggregationSuggestion cas = new CollectionAggregationSuggestion();
		
		// Classname which owns the collection
		cas.setClazzName(null);
		
		// fieldname of the collection in above class
		cas.setOwnerCollectionFieldName(null);
		
		// Classname of the items in the collection
		cas.setCollectionItemTypeName(null);
				
		// Name of the field over which was aggregated
		cas.setFieldName(null);
				
		return cas;
	}
	
	
	/**
	 * @param o array containing {classname,fieldname}
	 * @return
	 */
	public static FieldRemovalSuggestion getFRS(Object[] o) {
		FieldRemovalSuggestion frs = new FieldRemovalSuggestion();
		
		// class which owns the field
		frs.setClazzName((String) o[0]);
		
		// name of the field
		frs.setFieldName((String) o[1]);
		
		return frs;
	}
	
	
	/**
	 * @param o
	 * @return
	 */
	public static UnusedCollectionSuggestion getUCS(Object[] o) {
		UnusedCollectionSuggestion ucs = new UnusedCollectionSuggestion();
		
		return ucs;
	}
	
	

}
