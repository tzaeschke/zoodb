package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;

import org.zoodb.profiling.ProfilingConfig;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.LOBSuggestion;

public class LobCandidate implements ICandidate {
	
	private Double cost;
	private Double gain;
	
	private Class<?> clazz;
	
	private Field lobField;
	
	//total activations of 'clazz'
	private int totalActivations;
	
	//how many times the blob-class has been activated, but not been accessed 
	//--> overhead: blob was transferred to memory
	private int activationsNoLobRead;
	
	//how many times other attributes have been written but not the blob 
	//--> overhead: blob was transferred back to disk
	private int otherWritesNoLobWrite;
	
	//how many times the blob was accessed but no other attribute on the same object
	//--> overhead: sizeof(C)-avgBlobSize were transferred into memory
	private int lobAccessNoOtherAccess = 0;
	
	//how many times the blob was written but no other attributes
	//-->overhead: sizeof(C)--avgBlobSize were transferred back to disk
	private int lobWriteNoOtherWrite = 0;
	
	private double avgLobSize;
	
	private double avgClassSize;
	
	private int detectionCount;

	
	public Class<?> getClazz() {
		return clazz;
	}

	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}

	public Field getLobField() {
		return lobField;
	}

	public void setLobField(Field lobField) {
		this.lobField = lobField;
	}

	public int getTotalActivations() {
		return totalActivations;
	}

	public void setTotalActivations(int totalActivations) {
		this.totalActivations = totalActivations;
	}

	public int getActivationsNoLobRead() {
		return activationsNoLobRead;
	}

	public void setActivationsNoLobRead(int activationsNoLobRead) {
		this.activationsNoLobRead = activationsNoLobRead;
	}

	public int getOtherWritesNoLobWrite() {
		return otherWritesNoLobWrite;
	}

	public void setOtherWritesNoLobWrite(int otherWritesNoLobWrite) {
		this.otherWritesNoLobWrite = otherWritesNoLobWrite;
	}

	public int getLobAccessNoOtherAccess() {
		return lobAccessNoOtherAccess;
	}

	public void setLobAccessNoOtherAccess(int lobAccessNoOtherAccess) {
		this.lobAccessNoOtherAccess = lobAccessNoOtherAccess;
	}

	public int getLobWriteNoOtherWrite() {
		return lobWriteNoOtherWrite;
	}

	public void setLobWriteNoOtherWrite(int lobWriteNoOtherWrite) {
		this.lobWriteNoOtherWrite = lobWriteNoOtherWrite;
	}
	
	public int getDetectionCount() {
		return detectionCount;
	}

	public void setDetectionCount(int detectionCount) {
		this.detectionCount = detectionCount;
	}

	public double getAvgLobSize() {
		return avgLobSize;
	}

	public void setAvgLobSize(double avgLobSize) {
		this.avgLobSize = avgLobSize;
	}

	public double getAvgClassSize() {
		return avgClassSize;
	}

	public void setAvgClassSize(double avgClassSize) {
		this.avgClassSize = avgClassSize;
	}

	@Override
	public boolean evaluate() {
		//introducing a reference
		//the cost of a reference is for all activations, not only for the ones which don't access the lob attribute
		//assuming that to load the lob-attribute, you need to load the parent
		cost = ProfilingConfig.COST_NEW_REFERENCE * ((double) totalActivations);
		
		//lob not accessed
		gain = activationsNoLobRead*avgLobSize;
		gain += otherWritesNoLobWrite*avgLobSize;
		gain += lobWriteNoOtherWrite * (avgClassSize - avgLobSize);
		
		//this is a hard margin evaluation!
		return cost < gain;
	}

	@Override
	public double ratioEvaluate() {
		if (cost == null || gain == null) {
			evaluate(); //use evaluate to compute cost and gain
		}
		return gain/cost;
	}

	@Override
	public AbstractSuggestion toSuggestion() {
		LOBSuggestion ls = new LOBSuggestion();
		return ls;
	}
	
}