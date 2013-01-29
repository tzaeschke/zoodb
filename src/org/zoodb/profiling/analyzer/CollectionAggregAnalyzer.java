package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.jdo.api.DBCollection;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.CollectionActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

/**
 * @author tobiasg
 * Analyzes activations of a collection and its children
 * If every child is accessed and has only
 *
 */
public class CollectionAggregAnalyzer implements IAnalyzer {
	
	private Set<AggregationCandidate> candidatesReadOK;
	
	private IFieldManager fm = ProfilingManager.getInstance().getFieldManager(); 
	
	public CollectionAggregAnalyzer() {
		candidatesReadOK = new HashSet<AggregationCandidate>(); 
	}

	@Override
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions) {
		Collection<AbstractSuggestion> newSuggestions = new LinkedList<AbstractSuggestion>();
		
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			
			currentArchive = ProfilingManager.getInstance().getPathManager().getArchive(currentArchiveClass);
			Iterator<AbstractActivation> iter = currentArchive.getIterator();
			
			checkSingleArchive(iter,currentArchiveClass);
			
		}
		
		suggestions.addAll(newSuggestions);
		return newSuggestions;
	}
	
	/**
	 * Checks all activations for this archive for the 'aggregation-pattern'
	 * @param iter
	 * @param archiveClass
	 */
	private void checkSingleArchive(Iterator<AbstractActivation> iter, Class<?> archiveClass) {
		AbstractActivation aa = null;
		 while (iter.hasNext()) {
			 aa = iter.next();
			 
			 Object[] tmp = isCandidate(aa);
		 }
	}
			
			

		

//		for (Class<?> c : candidates.keySet()) {
//			int totalActivationsOf_c = ProfilingManager.getInstance().getPathManager().getArchive(c).size();
//			Object[] candidate = candidates.get(c);
//			long totalCollectionAndCollectionItemsBytes = (Long) candidate[2];
//			
//			//TODO: get fieldtype via reflection --> get fieldsize of fieldtype
//			// use 4 as default fieldsize
//			if (totalActivationsOf_c*4 < totalCollectionAndCollectionItemsBytes) {
//				
//				//TODO: 2nd argument should be fieldname of collection in collection owner class
//				//TODO: 5th argument should be fieldtype of collection-item
//				Object[] o = new Object[] {c.getName(),null,candidate[0],candidate[1],null,candidate[2],candidate[3],candidate[4]};
//				
//				/*
//				 * If there are writes, the aggregated field would be updated, calculate the costs of updating this field
//				 */
//				if (true) {
//					AggregationCandidate ac = new AggregationCandidate(c.getName(),(String) candidate[1], (String) candidate[0]);
//					ac.setBytes( (Long) candidate[2]);
//					ac.setItemCounter((Integer) candidate[3]);
//					ac.setPatternCounter((Integer) candidate[5]);
//					candidatesReadOK.put(c.getName(), ac);
//				}
//			}
//		}
//		if (candidatesReadOK.keySet().size() > 0) {
//			analyzeForWrite();
//			
//			for (AggregationCandidate rwCand : candidatesReadOK.values()) {
//				if (rwCand.evaluate()) {
//					suggestions.add(SuggestionFactory.getCAS(rwCand));
//				}
//			}
//		}
//		
//		
//		
//		return suggestions;
//	}
	
	
	private void analyzeForWrite() {
		Collection<AbstractSuggestion> suggestions = new LinkedList<AbstractSuggestion>();
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		

		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		CollectionActivation currentA = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			
			/*
			 * The following only works when using DBCollections --> e.g. LinkedList will not be detected
			 */
			if (DBCollection.class.isAssignableFrom(currentArchiveClass)) {
				currentArchive = ProfilingManager.getInstance().getPathManager().getArchive(currentArchiveClass);
				 
				 Iterator<AbstractActivation> iter = currentArchive.getIterator();
				 
				 while (iter.hasNext()) {
					 currentA = (CollectionActivation) iter.next();
					 
					 //check if this activation belongs to a read candidate
					 if (analyzableForWrite(currentA)) {
						 /*
						  * are there write field-accesses on the parent?
						  * 
						  */
						 AggregationCandidate ac = candidatesReadOK.get(currentA.getParent().getClazz().getName());
						 if (hasWriteAccessForParent(currentA)) {
							 ac.add2Writes(currentA.getTrx(), currentA.getParentOid());
						 } else {
							//are there writes on the collection itselft
							 if (hasWriteAccessByOidTrx(currentA.getOid(),currentA.getTrx())) {
								 ac.add2Writes(currentA.getTrx(), currentA.getParentOid());
							 } else {
								 //are there writes on the aggregation-field of any child (collection item)
								 AbstractActivation currentChild;
								 Iterator<AbstractActivation> childIter = currentA.getChildrenIterator();
								 
								 while (childIter.hasNext()) {
									 currentChild = childIter.next();
									 
									 if (hasWriteAccessByOidTrxField(currentChild.getOid(),currentChild.getTrx(),ac.getFieldName())) {
										 ac.add2Writes(currentA.getTrx(), currentA.getParentOid());
										 break;
									 }
								 }
							 }
						 }
						 
					 }

				 }
			}
		}
	}
	
	private boolean analyzableForWrite(AbstractActivation a) {
		return candidatesReadOK.keySet().contains(a.getParentClass().getName());
	}
	
	private boolean hasWriteAccessByOidTrxField(long oidUnderTest, String trxUnderTest, String fieldUnderTest) {
		Collection<IFieldAccess> fas = fm.get(oidUnderTest, trxUnderTest);
		for (IFieldAccess fa : fas) {
			if (fa.isWrite() && fa.getFieldName().equals(fieldUnderTest)) {
				return true;
			}
		}
		return false;
	}
	
	private boolean hasWriteAccessByOidTrx(long oidUnderTest, String trxUnderTest) {
		Collection<IFieldAccess> fas = fm.get(oidUnderTest, trxUnderTest);
		for (IFieldAccess fa : fas) {
			if (fa.isWrite()) {
				return true;
			}
		}
		return false;
	}
	
	
	private boolean hasWriteAccessForParent(AbstractActivation a) {
		if (a.getParent() != null) {
			long oidUnderTest = a.getParent().getOid();
			String trxUnderTest = a.getTrx();
			
			Collection<IFieldAccess> fas = fm.get(oidUnderTest, trxUnderTest);
			for (IFieldAccess fa : fas) {
				if (fa.isWrite()) {
					return true;
				}
			}	
		}
		return false;
	}

	/**
	 * Checks if all children of the activation 'ca' are leaves (have no children)
	 * and all access the same field.
	 * @param ca
	 * @return Object[] with {class of collection item, fieldName of collection item, number_of_aggregated_items_over,read/write flag}
	 */
	private Object[] isCandidate(AbstractActivation ca) {
		Iterator<AbstractActivation> childIter = ca.getChildrenIterator();
		IFieldManager fm = ProfilingManager.getInstance().getFieldManager(); 
		
		AbstractActivation currentChild = null;
		
		String fieldName = null;
		Class<?> assocClass = null;
		boolean isWriteAccess = false;
		
		Class<?> parentClass = ca.getClazz();
		
		List<List<AbstractActivation>> buckets = new LinkedList<List<AbstractActivation>>();
		
		while (childIter.hasNext()) {
			currentChild = childIter.next();
			
			
			
			//all children of 'ca' should be leaves!
			if (currentChild.getChildrenCount() != 0) {
				continue;
			}
			
			/*
			 * get fieldAccesses of (current.oid,current.trx)
			 * --> should be exactly 1, if not, abort
			 */
			Collection<IFieldAccess> fas = fm.get(currentChild.getOid(), currentChild.getTrx());
			
			if (fas.size() != 1) {
				continue;
			} else {
				/*
				 * The currentChild has a single fieldAcess and no children
				 * An aggregation on the parent could omit this activation
				 */
				// add the currentChild to the bucketList
				
				
				IFieldAccess fa = fas.iterator().next();
				fieldName = fa.getFieldName();
				assocClass = fa.getAssocClass();
				isWriteAccess = fa.isWrite();
				
				Field parentField = ReflectionUtils.getFieldForName(parentClass, currentChild.getParentFieldName());
				Class<?> aggregateeClass = currentChild.getClazz();
				Field aggregateeField = ReflectionUtils.getFieldForName(aggregateeClass, fieldName);
				
				AggregationCandidate ac = getCandidate(parentClass,parentField,aggregateeClass,aggregateeField);
				//ac.incItemCounter(increment);
				
				
			}
		}
		
		return new Object[] {assocClass,fieldName,isWriteAccess,1};
	}
	
	
	/**
	 * 
	 * @param buckets
	 * @param currentChild
	 */
	private void addToBuckets(List<List<AbstractActivation>> buckets, AbstractActivation currentChild) {
		
	}
	
	
	/**
	 * Returns the existing candidate which matches the arguments.
	 * Creates a new candidate and returns it if no such candidate exists
	 * @param parentClass
	 * @param parentField
	 * @param aggregateeClass
	 * @param aggregateeField
	 * @return
	 */
	public AggregationCandidate getCandidate(Class<?> parentClass, Field parentField,Class<?> aggregateeClass, Field aggregateeField) {
		AggregationCandidate ac = hasCandidate(parentClass,parentField,aggregateeClass,aggregateeField);
		
		if (ac == null) {
			ac = new AggregationCandidate();
			ac.setParentClass(parentClass);
			ac.setParentField(parentField);
			ac.setAggregateeClass(aggregateeClass);
			ac.setAggregateeField(aggregateeField);
			
			candidatesReadOK.add(ac);
		}
		return ac;
	}
	
	
	/**
	 * Returns the existing candidate that matches the arguments. 
	 * Returns null if such a candidate does not yet exist. 
	 * @param parentClass
	 * @param parentField
	 * @param aggregateeCLass
	 * @param aggregateeField
	 * @return
	 */
	private AggregationCandidate hasCandidate(Class<?> parentClass, Field parentField,Class<?> aggregateeClass, Field aggregateeField) {
		boolean same = true;
		for (AggregationCandidate ac : candidatesReadOK) {
			same = true;
			same &= ac.getParentClass() == parentClass;
			same &= ac.getParentField() == parentField;
			same &= ac.getAggregateeClass() == aggregateeClass;
			same &= ac.getAggregateeField() == aggregateeField;
			
			if (same) return ac; 
		}
		return null;
	}
	
	
		


	
	
	 
	


}
