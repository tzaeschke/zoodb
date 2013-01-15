package org.zoodb.profiling.analyzer;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class TrxGroup {
	
	private List<String> fields;
	
	private List<String> trxIds;
	
	private List<int[]> accessVectors;
	
	private FieldCount[] fieldCounts;
	
	private int splitIndex;
	
	public TrxGroup(List<String> fields) {
		this.fields = fields;
		
		trxIds = new LinkedList<String>();
		accessVectors = new LinkedList<int[]>();
		fieldCounts = new FieldCount[fields.size()];
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

	public boolean calculateSplit() {
		/*
		 * aggregate accesses per field
		 * outsource the fieldcount to a separate class so we can use the comparable interfaces
		 * and do not have to permute/sort 2 arrays with same order!
		 */
		int fieldsCount = fields.size();
		String fName = null;
		
		for (int i=0;i<fieldsCount;i++) {
			fName = fields.get(i);
			int aggrCount = 0;
			for (int[] av : accessVectors) {
				aggrCount += av[i];
			}
			fieldCounts[i] = new FieldCount(fName,aggrCount);
		}
		
		/*
		 * Sort field counts descending by field accesses
		 */
		Arrays.sort(fieldCounts);
		
		SplitStrategyAdvisor ssa = new SplitStrategyAdvisor(new SimpleSplitStrategy());
		splitIndex = ssa.checkForSplit(fieldCounts);
		
		if (splitIndex > 0) {
			return true;
		} else {
			return false;
		}
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

	public int getSplitIndex() {
		return splitIndex;
	}
	
	

}