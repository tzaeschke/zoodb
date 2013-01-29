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
	 * The number of known additional writes on this pattern 
	 */
	private int additionalWrites;
	
	
	/**
	 * Over how many items did we aggregate (separate number for each time the pattern occured)
	 * The length of this list indicates the number of times the pattern has occurred 
	 */
	private List<Integer> itemCounter;
	
	
	
	
	
	public AggregationCandidate() {
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

	@Override
	public boolean evaluate() {
		// TODO Auto-generated method stub
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