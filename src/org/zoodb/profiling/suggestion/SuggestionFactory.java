package org.zoodb.profiling.suggestion;

import java.util.List;

public class SuggestionFactory {
	
	
	/**
	 * @param o
	 * @return
	 */
	public static CollectionAggregationSuggestion getCAS(Object[] o) {
		CollectionAggregationSuggestion cas = new CollectionAggregationSuggestion();
		
		// Classname which owns the collection
		cas.setClazzName((String) o[0]);
		
		// fieldname of the collection in above class
		cas.setOwnerCollectionFieldName((String) o[1]);
		
		// Classname of the items in the collection
		cas.setCollectionItemTypeName((String) o[2]);
				
		// Name of the field over which was aggregated
		cas.setFieldName((String) o[3]);
		
		cas.setTotalCollectionBytes((Long) o[5]);
				
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
	 * @param o arraz containing {classname which owns collection, fieldname of owner, sum of all collectionbytes, fieldname of collection which triggered activation}
	 * @return
	 */
	public static UnusedCollectionSuggestion getUCS(Object[] o) {
		UnusedCollectionSuggestion ucs = new UnusedCollectionSuggestion();
		
		// Classname which owns the collection
		ucs.setClazzName((String) o[0]);
		
		// fieldname of the collection in above class
		ucs.setFieldName((String) o[1]);
		
		// sum of all collectionbytes
		ucs.setTotalCollectionBytes((Long) o[2]);
		
		// fieldname of collection which triggered activation
		ucs.setTriggerName((String) o[3]);
		
		return ucs;
	}
	
	
	/**
	 * 
	 * @param o
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static ReferenceShortcutSuggestion getRSS(Object[] o) {
		ReferenceShortcutSuggestion rss = new ReferenceShortcutSuggestion();
		
		// class where the new reference would be introduced
		rss.setClazzName((String) o[0]);
		
		// class which would be the target of the new reference
		rss.setRefTarget((String) o[1]);
		
		// classnames of intermediary nodes
		rss.setIntermediates((List<String>) o[3]);
		
		return rss;
	}
	
	

}
