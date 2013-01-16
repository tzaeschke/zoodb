package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class AnalyzerPipeline {
	
private List<IAnalyzer> analyzers;
	
	private Collection<AbstractSuggestion> suggestions;
	
	public AnalyzerPipeline() {
		analyzers = new LinkedList<IAnalyzer>();
	}
	
	public void addAnalyzer(IAnalyzer a) {
		analyzers.add(a);
	}
	
	public void startPipeline() {
		if (analyzers != null) {
			
			suggestions = new LinkedList<AbstractSuggestion>();
			
			for (IAnalyzer a : analyzers) {
				a.analyze(suggestions);
			}
		}
	}
	
	public Collection<AbstractSuggestion> getSuggestions() {
		return suggestions;
	}
	
}