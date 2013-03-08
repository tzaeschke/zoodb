package org.zoodb.profiling.api;

import java.util.Collection;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public interface IProfilingManager {
	
	public void save();
	
	public IPathManager getPathManager();
	
	public IFieldManager getFieldManager();
	
	public ITrxManager getTrxManager();
	
	/**
	 * Setup profiling
	 */
	public void init(String tag);
	
	/**
	 * Finalize profiling data
	 */
	public void finish();
	
	public void addSuggestion(AbstractSuggestion s);
	
	public void addSuggestions(Collection<AbstractSuggestion> s);
	
}
