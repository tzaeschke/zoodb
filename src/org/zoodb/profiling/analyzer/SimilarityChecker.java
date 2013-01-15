package org.zoodb.profiling.analyzer;

public class SimilarityChecker {
	
	private ISimilarityStrategy strategy;
	
	public SimilarityChecker(ISimilarityStrategy s) {
		this.strategy = s;
	}
	
	public boolean check(int[] accessVector, TrxGroup group) {
		return strategy.executeStrategy(accessVector, group);
	}

}
