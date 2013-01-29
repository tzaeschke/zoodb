package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;


/**
 * @author tobias
 *
 */
public class AggregationCandidate implements ICandidate {
	
	/**
	 * Name of the class in which an aggregated field would be introduced 
	 */
	private Class<?> parentClass;
	
	/**
	 * Field in the parent class which holds the aggregateeClass (or a collection of them) 
	 */
	private Field parentField;
	
	/**
	 * Name of the class which contains the field upon which has been aggregated 
	 */
	private Class<?> aggregateeClass;
	
	/**
	 * Name of the field in the collection-item class upon which has been aggregated 
	 */
	private Field aggregateeField;
	
	/**
	 * The write set of on the parentClass: contains (oid,trx) tuples 
	 */
	private Collection<Object[]> parentWrites;
	
	
	/**
	 * Over how many items did we aggregate (separate number for each time the pattern occured)
	 * The length of this list indicates the number of times the pattern has occurred 
	 */
	private List<Integer> itemCounter;
	
	
	
	public AggregationCandidate() {
		parentWrites = new LinkedList<Object[]>();
		itemCounter = new LinkedList<Integer>();
	}
	

	public Class<?> getParentClass() {
		return parentClass;
	}
	public void setParentClass(Class<?> parentClass) {
		this.parentClass = parentClass;
	}
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
	public List<Integer> getItemCounter() {
		return itemCounter;
	}
	public void setItemCounter(List<Integer> itemCounter) {
		this.itemCounter = itemCounter;
	}


	/**
	 * Updates the item counter. If a pattern for this candidate occurs, 
	 * analyzers can use this method to indicate over how many items
	 * have been aggregated in the new pattern-occurence
	 * @param increment
	 */
	public void incItemCounter(int increment) {
		itemCounter.add(increment);
	}


	/**
	 * Adds the pair (oid,trx) to the write set only if it does not exist yet (--> additional write)
	 * @param trx
	 * @param oid
	 */
	public void add2Writes(String trx, long oid) {
		for (Object[] o : parentWrites) {
			if (o[0].equals(oid) && o[1].equals(trx)) {
				return;
			}
		}
		parentWrites.add(new Object[] {oid,trx});
	}

	public Collection<Object[]> getParentWrites() {
		return parentWrites;
	}

	public void setParentWrites(Collection<Object[]> parentWrites) {
		this.parentWrites = parentWrites;
	}


	/** 
	 * Returns true if the gain is still higher than the cost (including the costs introduced by writes)
	 * @return
	 */
	public boolean evaluate() {
		int writeCount = parentWrites.size();
		try {
			int totalActivationsOfParent = ProfilingManager.getInstance().getPathManager().getArchive(Class.forName(parentClass)).size();
			
			if ( (totalActivationsOfParent + writeCount)*4 < bytes) {
				return true;
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public double ratioEvaluate() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public AbstractSuggestion toSuggestion() {
		// TODO Auto-generated method stub
		return null;
	}
	
}