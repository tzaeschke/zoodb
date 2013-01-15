package org.zoodb.profiling.analyzer;

import java.util.LinkedList;
import java.util.List;

public class TrxGroup {
	
	private List<String> fields;
	
	private List<String> trxIds;
	
	private List<int[]> accessVectors;
	
	
	public TrxGroup(List<String> fields) {
		this.fields = fields;
		
		trxIds = new LinkedList<String>();
		accessVectors = new LinkedList<int[]>();
	}
	
	/**
	 * Adds a transaction to this transaction group
	 * @param trxId
	 * @param accessVectors
	 */
	public void addTrx(String trxId, int[] accessVector) {
		trxIds.add(trxId);
		accessVectors.add(accessVector);
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public List<String> getTrxIds() {
		return trxIds;
	}

	public void setTrxIds(List<String> trxIds) {
		this.trxIds = trxIds;
	}

	public List<int[]> getAccessVectors() {
		return accessVectors;
	}

	public void setAccessVectors(List<int[]> accessVectors) {
		this.accessVectors = accessVectors;
	}

}