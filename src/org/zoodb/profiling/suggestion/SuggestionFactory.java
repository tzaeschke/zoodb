package org.zoodb.profiling.suggestion;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.analyzer.AggregationCandidate;
import org.zoodb.profiling.analyzer.ClassMergeCandidate;
import org.zoodb.profiling.analyzer.TrxGroup;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassMergeSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.CollectionAggregationSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldDataTypeSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldRemovalSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.LOBSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ReferenceShortcutSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.SplitClassSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.UnusedCollectionSuggestion;

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
	
	public static AbstractSuggestion getLS(Class<?> c, Field f,int detectionCount,int accessCount) {
		LOBSuggestion ls = new LOBSuggestion();
		
		ls.setClazzName(c.getName());
		ls.setFieldName(f.getName());
		ls.setAccessCount(accessCount);
		ls.setDetectionCount(detectionCount);
		
		return ls;
	}
	


	public static AbstractSuggestion getCAS(AggregationCandidate rwCand) {
		CollectionAggregationSuggestion cas = new CollectionAggregationSuggestion();
		
		// Classname which owns the collection
		cas.setClazzName(rwCand.getParentClass());
		
		// fieldname of the collection in above class
		cas.setOwnerCollectionFieldName(null);
		
		// Classname of the items in the collection
		cas.setCollectionItemTypeName(rwCand.getAssocClass());
		
		// Total number of items which was aggregated upon
		cas.setNumberOfCollectionItems(rwCand.getItemCounter());
				
		// Name of the field over which was aggregated
		cas.setFieldName(rwCand.getFieldName());
		
		cas.setTotalCollectionBytes(rwCand.getBytes());
		
		cas.setNumberOfAggregations(rwCand.getPatternCounter());
				
		return cas;
	}


	public static AbstractSuggestion getCSS(Class<?> c, TrxGroup maxGainGroup) {
		SplitClassSuggestion css = new SplitClassSuggestion();
		
		css.setClazzName(c.getName());
		
		css.setCost(maxGainGroup.getCost());
		css.setGain(maxGainGroup.getGain());
		css.setBenefitTrxs(maxGainGroup.getTrxIds());
		css.setOutsourcedFields(maxGainGroup.getSplittedFields());
		
		
		return css;
	}
	
	public static AbstractSuggestion getCMS(ClassMergeCandidate candidate) {
		ClassMergeSuggestion cms = new ClassMergeSuggestion();
		
		cms.setClazzName(candidate.getMaster().getName());
		
		cms.setMasterClass(candidate.getMaster().getName());
		cms.setMergeeClass(candidate.getMergee().getName());
		
		cms.setTotalMasterActivations(candidate.getTotalMasterActivations());
		cms.setTotalMergeeActivations(candidate.getTotalMergeeActivations());
		
		cms.setSizeOfMaster(candidate.getSizeOfMaster());
		cms.setSizeOfMergee(candidate.getSizeOfMergee());
		
		cms.setFieldName(candidate.getField().getName());
		
		cms.setMasterWMergeeRead(candidate.getMasterWMergeeRead());
		cms.setMergeeWOMasterRead(candidate.getMergeeWOMasterRead());
		
		return cms;
	}
	

}
