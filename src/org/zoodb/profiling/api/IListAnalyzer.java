package org.zoodb.profiling.api;

import java.util.Collection;

import org.zoodb.profiling.api.tree.impl.ClazzNode;

import ch.ethz.globis.profiling.commons.suggestion.ListSuggestion;

public interface IListAnalyzer {
	
	public Collection<ListSuggestion> analyzeList(ClazzNode listTree);
	
}
