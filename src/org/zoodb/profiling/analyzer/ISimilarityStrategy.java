package org.zoodb.profiling.analyzer;

public interface ISimilarityStrategy {
	
	boolean executeStrategy(int[] accessVector,TrxGroup g);

}
