package org.zoodb.profiling.suggestion;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.analyzer.AggregationCandidate;
import org.zoodb.profiling.analyzer.ClassMergeCandidate;
import org.zoodb.profiling.analyzer.TrxGroup;
import org.zoodb.profiling.analyzer.UnusedFieldCandidate;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassMergeSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.CollectionAggregationSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldDataTypeSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldRemovalSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.LOBSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ReferenceShortcutSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassSplitSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.UnusedCollectionSuggestion;

public class SuggestionFactory {
	
	
	/**
	 * Returns a field removal suggestion.
	 * @param name: name of the class
	 * @param ifn: name of the field
	 * @param isInheritedField: whether 'ifn' is an inherited field;
	 * @param nameSuper: name of the superclass (if inherited)
	 * @return
	 */
	public static FieldRemovalSuggestion getFRS(UnusedFieldCandidate ufc) {
		FieldRemovalSuggestion frs = new FieldRemovalSuggestion();
		
		frs.setClazzName(ufc.getClazz().getName());
		frs.setFieldName(ufc.getF().getName());
		
		if (ufc.getSuperClazz() != null) {
			frs.setClazzNameSuper(ufc.getSuperClazz().getName());
		}
		frs.setFieldTypeName(ufc.getF().getType().getSimpleName());
		frs.setTotalActivations(ufc.getTotalActivationsClazz());
		frs.setTotalWrites(ufc.getTotalWritesClazz());
		
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
