package org.zoodb.profiling.analyzer;

import java.util.HashSet;
import java.util.Set;

public class TrxSet {
	
	private Set<String> trxIds;
	
	public TrxSet() {
		trxIds = new HashSet<String>();
	}
	
	public void addTrx(String trxId) {
		trxIds.add(trxId);
	}
	
	public boolean hasTrx(String id) {
		return trxIds.contains(id);
	}
	
	
	public int getOverlapCount(TrxSet other) {
		int overlapCount = 0;
		
		for (String s : trxIds) {
			if (other.hasTrx(s)) {
				overlapCount++;
			}
		}
		
		return overlapCount;
	}

}
