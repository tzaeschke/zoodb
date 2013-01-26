package org.zoodb.profiling.analyzer;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public interface ICandidate {
	
	/**
	 * Hard margin evaluation: returns true if the gain for the suggested operation is strictly 
	 * greater than the cost. 
	 * @return true iif cost < gain
	 */
	public boolean evaluate();
	
	/**
	 * Returns the gain/cost ratio
	 * @return
	 */
	public double ratioEvaluate();
	
	/**
	 * Creates a suggestion out of this candidate
	 * @return
	 */
	public AbstractSuggestion toSuggestion();

}
