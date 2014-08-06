package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;

import org.zoodb.profiling.ProfilingConfig;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassMergeSuggestion;

public class ClassMergeCandidate implements ICandidate {
	
	private Class<?> master;
	private Class<?> mergee;
	
	/**
	 * The field in the master class which holds the attribute of type mergee.
	 */
	private Field f;
	
	/**
	 * The number of times objects of type mergee have been activated without coming from the master.
	 * 
	 */
	private int mergeeWOMasterRead;  
	
	private Double cost;
	private Double gain;


	/**
	 * The number of times objects of the master-class have been activated without traversing the branch to the mergee-class 
	 */
	//actually we can compute this field with totalMasterActivations - masterWMergeeRead, simplifies code!
	private int masterWMergeeRead; 
	
	private int totalMasterActivations;
	private int totalMergeeActivations;
	
	private double sizeOfMergee;
	private double sizeOfMaster;
	
	
	public ClassMergeCandidate(Class<?> master,Class<?> mergee,Field f) {
		this.f = f;
		this.master = master;
		this.mergee = mergee;
	}
	
	public void incMergeeWOMasterRead() {
		mergeeWOMasterRead++;
	}
	
	public void incMasterWMergeeRead() {
		this.masterWMergeeRead++;
	}
	
	/**
	 * Returns true if the costs for this candidate are lower than the gain. This means a class merge of master with mergee should increase performance
	 * @return
	 */
	public boolean evaluate() {
		double gainTerm1 = ProfilingConfig.CMA__GAMMA*totalMasterActivations*ProfilingConfig.COST_NEW_REFERENCE;
		double gainTerm2 = ProfilingConfig.CMA_DELTA*totalMergeeActivations*ProfilingConfig.COST_NEW_REFERENCE;
		double costTerm1 = ProfilingConfig.CMA_ALPHA*mergeeWOMasterRead*sizeOfMaster;
		double costTerm2 = ProfilingConfig.CMA_BETA*getMasterWOMergeeRead()*sizeOfMergee;
		
		gain = gainTerm1 + gainTerm2;
		cost = costTerm1 + costTerm2;
		
		return costTerm1 + costTerm2 < gainTerm1 + gainTerm2;
	}
	
	/*
	 * Getter/Setter
	 */
	public Class<?> getMaster() {
		return master;
	}

	public Class<?> getMergee() {
		return mergee;
	}

	public int getMergeeWOMasterRead() {
		return mergeeWOMasterRead;
	}

	public int getMasterWOMergeeRead() {
		return totalMasterActivations - masterWMergeeRead;
	}

	public int getTotalMasterActivations() {
		return totalMasterActivations;
	}

	public void setTotalMasterActivations(int totalMasterActivations) {
		this.totalMasterActivations = totalMasterActivations;
	}

	public int getTotalMergeeActivations() {
		return totalMergeeActivations;
	}

	public void setTotalMergeeActivations(int totalMergeeActivations) {
		this.totalMergeeActivations = totalMergeeActivations;
	}
	
	public Field getField() {
		return f;
	}
	
	public int getMasterWMergeeRead() {
		return this.masterWMergeeRead;
	}
	
	public void setMergeeWOMasterRead(int mergeeWOMasterRead) {
		this.mergeeWOMasterRead = mergeeWOMasterRead;
	}

	public double getSizeOfMergee() {
		return sizeOfMergee;
	}

	public void setSizeOfMergee(double sizeOfMergee) {
		this.sizeOfMergee = sizeOfMergee;
	}

	public double getSizeOfMaster() {
		return sizeOfMaster;
	}

	public void setSizeOfMaster(double sizeOfMaster) {
		this.sizeOfMaster = sizeOfMaster;
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
		ClassMergeSuggestion cms = new ClassMergeSuggestion();
		
		cms.setClazzName(getMaster().getName());
		
		cms.setMasterClass(getMaster().getName());
		cms.setMergeeClass(getMergee().getName());
		
		cms.setTotalMasterActivations(getTotalMasterActivations());
		cms.setTotalMergeeActivations(getTotalMergeeActivations());
		
		cms.setSizeOfMaster(getSizeOfMaster());
		cms.setSizeOfMergee(getSizeOfMergee());
		
		cms.setFieldName(getField().getName());
		
		cms.setMasterWMergeeRead(getMasterWMergeeRead());
		cms.setMergeeWOMasterRead(getMergeeWOMasterRead());
		
		cms.setCost(cost);
		cms.setGain(gain);

		return cms;
	}

}