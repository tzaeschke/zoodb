package org.zoodb.profiling.suggestion;

import java.util.LinkedList;
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
		
		// Total number of items which was aggregated upon
		cas.setNumberOfCollectionItems((Integer) o[6]);
				
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
		List<Class<?>> intermediates = (List<Class<?>>) o[3];
		List<String> intermediatesClassNames = new LinkedList<String>();
		for (Class<?> c : intermediates) {
			intermediatesClassNames.add(c.getName());
		}
		rss.setIntermediates(intermediatesClassNames);
		
		// visitcounters of intermediary nodes
		rss.setIntermediatesVisitCounter((List<Integer>) o[4]);
		
		return rss;
	}
	
	
	/**
	 * @param o
	 * @return
	 */
	public static FieldDataTypeSuggestion getFDTS(Object[] o) {
		FieldDataTypeSuggestion fdts = new FieldDataTypeSuggestion();
		
		// class name
		fdts.setClazzName((String) o[0]);
		
		// name of the field
		fdts.setFieldName((String) o[1]);
		
		// name of new type
		fdts.setSuggestedType((String) o[2]);
		
		return fdts;
	}
	
	
	/**
	 * @param o
	 * @return
	 */
	public static CollectionAccessRefSuggestion getCARS(Object[] o) {
		CollectionAccessRefSuggestion cars = new CollectionAccessRefSuggestion();
		
		// class name
		cars.setClazzName((String) o[0]);
		
		// field name of collection
		cars.setFieldName((String) o[1]);
		
		// class name of collection items
		cars.setItemClazzName((String) o[2]);
		
		return cars;
	}
	
	
	
	
	

}
