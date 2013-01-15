package org.zoodb.profiling.analyzer;

import java.util.Collection;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public interface IAnalyzer {
	
	public Collection<AbstractSuggestion> analzye(Collection<AbstractSuggestion> suggestions);

}