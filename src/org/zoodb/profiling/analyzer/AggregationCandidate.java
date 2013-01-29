package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.impl.ClassSizeManager;
import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.CollectionAggregationSuggestion;


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
	
	private Double cost;
	private Double gain;
	
	private int totalActivationsParent;
	private int totalWritesParent;
	
	private double avgSizeOfAggregateeField;
	private double avgSizeOfAggregateeClass;
	private double avgSizeOfParentClass;
	
	
	
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
	public int getAdditionalWrites() {
		return additionalWrites;
	}
	public void setAdditionalWrites(int additionalWrites) {
		this.additionalWrites = additionalWrites;
	}
	public Double getCost() {
		return cost;
	}
	public void setCost(Double cost) {
		this.cost = cost;
	}
	public Double getGain() {
		return gain;
	}
	public void setGain(Double gain) {
		this.gain = gain;
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
	 * Increases the additionalWriteCounter by one. 
	 */
	public void incAdditionalWrite() {
		additionalWrites++;
	}
	

	@Override
	public boolean evaluate() {
		ClassSizeManager csm = ProfilingManager.getInstance().getClassSizeManager();
		
		IPathManager pm = ProfilingManager.getInstance().getPathManager();
		
		//get size of aggregateeField
		avgSizeOfAggregateeField = csm.getClassStats(aggregateeClass).getAvgFieldSizeForField(aggregateeField.getName());
		
		//totalactivations of parent
		totalActivationsParent = pm.getArchive(parentClass).size();
		
		avgSizeOfParentClass = pm.getArchive(parentClass).getAvgObjectSize();
				
		//totalWrites on parent
		totalWritesParent = ProfilingManager.getInstance().getFieldManager().getWriteCount(parentClass);
				
		cost = totalActivationsParent * avgSizeOfAggregateeField;
		cost += totalWritesParent * avgSizeOfAggregateeField;
		cost += additionalWrites*avgSizeOfAggregateeField;
		
		int totalPatternOccurence = 0;
		for (Integer i : itemCounter) {
			totalPatternOccurence += i;
		}
		avgSizeOfAggregateeClass = pm.getArchive(aggregateeClass).getAvgObjectSize();
		
		gain = totalPatternOccurence*avgSizeOfAggregateeClass;
		
		
		return cost < gain;
	}

	@Override
	public double ratioEvaluate() {
		if (cost == null || gain == null) {
			evaluate();
		}
		return gain/cost;
	}

	@Override
	public AbstractSuggestion toSuggestion() {
		CollectionAggregationSuggestion cas = new CollectionAggregationSuggestion();
		
		cas.setClazzName(parentClass.getName());
		cas.setParentField(parentField.getName());
		cas.setAggregateeClass(aggregateeClass.getName());
		cas.setAggregateeField(aggregateeField.getName());
		
		cas.setCost(cost);
		cas.setGain(gain);
		
		cas.setTotalActivations(totalActivationsParent);
		cas.setTotalWrites(totalWritesParent);
		cas.setAdditionalWrites(additionalWrites);
		
		cas.setAvgSizeOfAggregateeField(avgSizeOfAggregateeField);
		cas.setAvgSizeOfAggregateeClass(avgSizeOfAggregateeClass);
		
		cas.setAvgClassSize(avgSizeOfParentClass);
		cas.setItemCounter(itemCounter);

		
		
		return cas;
	}


	
	
}