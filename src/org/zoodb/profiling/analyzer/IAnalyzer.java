package org.zoodb.profiling.analyzer;

import java.util.Collection;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

/**
 * To be part of the analyzer pipeline, each analyzer should implement this interface.
 * @author tobiasg
 *
 */
public interface IAnalyzer {
	
	/**
	 * Returns a list of suggestions (or null) specific for the implementing analyzer. 
	 * The analyzer has access to the already created suggestions previously in the pipeline.
	 * The analyzer can use them and add its new suggestions directly to the suggestions-collection. 
	 * @param suggestions: the previously created suggestions in the pipeline
	 * @return
	 */
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions);

}