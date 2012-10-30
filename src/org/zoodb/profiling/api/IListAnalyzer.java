package org.zoodb.profiling.api;

import java.util.Collection;

import org.zoodb.profiling.api.impl.ListSuggestion;
import org.zoodb.profiling.api.tree.impl.ClazzNode;

public interface IListAnalyzer {
	
	public Collection<ListSuggestion> analyzeList(ClazzNode listTree);
	
}
