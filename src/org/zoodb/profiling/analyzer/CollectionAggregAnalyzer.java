package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.profiling.ProfilingConfig;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.Utils;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.impl.SimpleFieldAccess;

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
	
	private Logger logger = ProfilingManager.getProfilingLogger();
	
	private ZooClassDef currentClsDef;
	private ZooClassDef currentChildClsDef;
	
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
			
			this.currentClsDef = currentArchive.getZooClassDef();
			checkSingleArchive(iter,currentArchiveClass);
			
		}
		
		//create suggestions
		for (AggregationCandidate ac : candidatesReadOK) {
			if (ac.ratioEvaluate() >= ProfilingConfig.ANALYZERS_GAIN_COST_RATIO_THRESHOLD) {
				newSuggestions.add(ac.toSuggestion());
			}
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
			 
			 isCandidate(aa);
		 }
	}
			
	
	private boolean hasWriteAccessByOidTrxField(AbstractActivation a, String fieldUnderTest) {
		if (a.getFas() != null) {
			ZooClassDef cd = ProfilingManager.getInstance().getPathManager().getArchive(a.getClazz()).getZooClassDef();
			
			int idx = Utils.getIndexForFieldName(fieldUnderTest, cd);
			SimpleFieldAccess sfa = a.getFas().get(idx);
			if (sfa != null && sfa.getwCount() > 0) {
				return true;
			}
		}
		return false;
	}
	

	/**
	 * Checks if all children of the activation 'ca' are leaves (have no children)
	 * and all access the same field.
	 * @param parentActivation
	 */
	private void isCandidate(AbstractActivation parentActivation) {
		Iterator<AbstractActivation> childIter = parentActivation.getChildrenIterator();
		
		//if the childIter is null, we do not have any children
		if (childIter == null) {
			return;
		}
		
		IFieldManager fm = ProfilingManager.getInstance().getFieldManager(); 
		
		AbstractActivation currentChild = null;
		
		String fieldName = null;
		
		/*
		 * If the parent has already a write access, a write on any children would not trigger an additional write
		 */
		boolean writeOnParent = parentActivation.hasWriteAccess();		
		
		List<Bucket> buckets = new LinkedList<Bucket>();
		while (childIter.hasNext()) {
			currentChild = childIter.next();
			this.currentChildClsDef = ProfilingManager.getInstance().getPathManager().getArchive(currentChild.getClazz()).getZooClassDef();
			
			//all children of 'ca' should be leaves! (only for the best case)
			if (currentChild.getChildrenCount() != 0) {
				continue;
			}
			
			/*
			 * get fieldAccesses of (current.oid,current.trx)
			 * --> should be exactly 1, if not, abort
			 */
			//Collection<IFieldAccess> fas = fm.get(currentChild.getOid(), currentChild.getTrx());
			Collection<SimpleFieldAccess> fas = currentChild.getFas().values();
			
			if (fas.size() != 1) {
				continue;
			} else {
				/*
				 * The currentChild has a single fieldAcess and no children
				 * An aggregation on the parent could omit this activation
				 */
				SimpleFieldAccess fa = fas.iterator().next();
				fieldName = Utils.getFieldNameForIndex(fa.getIdx(), this.currentChildClsDef);
				Class<?> aggregateeClass = currentChild.getClazz();
				Field aggregateeField = ReflectionUtils.getFieldForName(aggregateeClass, fieldName);
				
				//add the currentChild to the bucketList
				addToBuckets(buckets,currentChild,aggregateeField);
			}
		}
		
		/*
		 * Each bucket now contains the activations upon which was aggregated.
		 * For each of this bucket, we update the candidates
		 */
		updateCandidates(buckets,parentActivation,writeOnParent);
	}
	
	private void updateCandidates(List<Bucket> buckets, AbstractActivation parentActivation,boolean writeOnParent) {
		for (Bucket b : buckets) {
			
			//get the corresponding target candidate and update it			
			AggregationCandidate tc = getCandidate(parentActivation.getClazz(),b.getParentField(),b.getAggregateeClass(),b.getAggregateeField());
			tc.incItemCounter(b.size());
			
			//update the additional writes
			//we only count the number of writes which occur in the pattern context! The true number of additional writes
			//may be higher (also, there might be additional loads to recompute the aggregated field)
			if (!writeOnParent) {
				//check if any activation in the bucket has a write on the aggregation field
				Iterator<AbstractActivation> biIter = b.getItemIterator();
				while (biIter.hasNext()) {
					AbstractActivation aa = biIter.next();
					
					if (hasWriteAccessByOidTrxField(aa,b.getAggregateeField().getName())) {
					//if (hasWriteAccessByOidTrxField(aa.getOid(),aa.getTrx(),b.getAggregateeField().getName())) {
						//we have an additional write on the parent!
						//it is safe to break, because any other writes would be in the same trx and not trigger an additional write
						tc.incAdditionalWrite();
						break;
					}
				}
			}
				
			
		}
	}
	
	
	/**
	 * Checks if we allready have similar childs, if yes, adds the currentChild to the list with similar childs.
	 * A similar child is a child
	 * @param buckets
	 * @param currentChild
	 */
	private void addToBuckets(List<Bucket> buckets, AbstractActivation currentChild, Field aggregateeField) {
		Field parentField = ReflectionUtils.getFieldForName(currentChild.getParentClass(), currentChild.getParentFieldName());
		boolean same = true;
		for (Bucket b : buckets) {
			same = true;
			
			same &= b.getAggregateeClass() == currentChild.getClazz();
			same &= b.getAggregateeField().getName().equals(aggregateeField.getName());
			same &= b.getParentField().getName().equals(parentField.getName());
			
			if (same) {
				b.add2Items(currentChild);
				return;
			} else {
				continue;
			}
		}
		//if we have not returned so far, create a new bucket and add currentChild
		Bucket newBucket = new Bucket();
		newBucket.setAggregateeClass(currentChild.getClazz());
		newBucket.setAggregateeField(aggregateeField);
		newBucket.setParentField(parentField);
		newBucket.add2Items(currentChild);
		buckets.add(newBucket);
		
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
			same &= ac.getParentField().getName().equals(parentField.getName());
			same &= ac.getAggregateeClass() == aggregateeClass;
			same &= ac.getAggregateeField().getName().equals(aggregateeField.getName());
			
			if (same) return ac; 
		}
		return null;
	}
	
	
	/**
	 * Class to hold activations for a single parentActivation 
	 * A bucket is a container which holds activations that are similar in the following sense:
	 *  - same parentField
	 *  - same aggregateeClass
	 *  - same aggregateeField
	 *
	 */
	public class Bucket {
		private Field parentField;
		private Class<?> aggregateeClass; 
		private Field aggregateeField;
		private List<AbstractActivation> items = new LinkedList<AbstractActivation>();
		
		public Field getParentField() {
			return parentField;
		}
		public void setParentField(Field parentField) {
			this.parentField = parentField;
		}
		public Class<?> getAggregateeClass() {
			return aggregateeClass;
		}
		public void setAggregateeClass(Class<?> aggregateeClass) {
			this.aggregateeClass = aggregateeClass;
		}
		public Field getAggregateeField() {
			return aggregateeField;
		}
		public void setAggregateeField(Field aggregateeField) {
			this.aggregateeField = aggregateeField;
		}
		
		public void add2Items(AbstractActivation a) {
			items.add(a);
		}
		
		public Iterator<AbstractActivation> getItemIterator() {
			return items.iterator();
		}
		
		public int size() {
			return items.size();
		}
	}
	
}