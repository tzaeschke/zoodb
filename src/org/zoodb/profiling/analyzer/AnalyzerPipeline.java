package org.zoodb.profiling.analyzer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class AnalyzerPipeline {
	
	private List<IAnalyzer> analyzers;
	
	private Collection<AbstractSuggestion> suggestions;
	
	public AnalyzerPipeline() {
		analyzers = new ArrayList<IAnalyzer>();
	}
	
	public void addAnalyzer(IAnalyzer a) {
		analyzers.add(a);
	}
	
	public void startPipeline() {
		if (analyzers != null) {
			
			suggestions = new ArrayList<AbstractSuggestion>();
			
			for (IAnalyzer a : analyzers) {
				suggestions.addAll(a.analyze());
			}
		}
	}
	
	public Collection<AbstractSuggestion> getSuggestions() {
		return suggestions;
	}
	
}