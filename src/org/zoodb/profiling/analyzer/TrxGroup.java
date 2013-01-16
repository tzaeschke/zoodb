package org.zoodb.profiling.analyzer;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.ProfilingConfig;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ClassSizeStats;
import org.zoodb.profiling.api.impl.ProfilingManager;

public class TrxGroup {
	
	private List<String> fields;
	
	private List<String> trxIds;
	
	private List<int[]> accessVectors;
	
	private FieldCount[] fieldCounts;
	
	private int splitIndex;
	
	private long gain;
	private long cost;
	
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
	
	public boolean calculateSplitCost(Class<?> c) {
		ActivationArchive aa = ProfilingManager.getInstance().getPathManager().getArchive(c);
		int activationCount = aa.getActivationCountByTrx(getTrxIds());
		
		ClassSizeStats css = ProfilingManager.getInstance().getClassSizeManager().getClassStats(c);
		
		double outsourceCost = 0;
		gain = 0;
		
		//for each field that would be in the splittee-class, calculate its avg. cost
		for (int i=splitIndex;i<fieldCounts.length;i++) {
			int count = fieldCounts[i].getCount();
			double avgSize = css.getAvgFieldSizeForField(fieldCounts[i].getName());
			
			gain += activationCount*avgSize;
			outsourceCost += count*avgSize;
			
		}
		gain = activationCount*gain;
		outsourceCost = outsourceCost + activationCount*ProfilingConfig.COST_NEW_REFERENCE;
		cost = (long) outsourceCost;
		if (gain > outsourceCost) {
			return true;
		} else {
			return false;
		}
	}
	
	public List<String> getFields() {
		return fields;
	}
	public List<String> getTrxIds() {
		return trxIds;
	}
	public List<int[]> getAccessVectors() {
		return accessVectors;
	}
	public int getSplitIndex() {
		return splitIndex;
	}
	public FieldCount[] getFieldCounts() {
		return fieldCounts;
	}
	public long getGain() {
		return gain;
	}
	public long getCost() {
		return cost;
	}
	public Collection<String> getSplittedFields() {
		Collection<String> result = new LinkedList<String>();
		for (int i=splitIndex;i<fieldCounts.length;i++) {
			result.add(fieldCounts[i].getName());
		}
		return result;
	}
}