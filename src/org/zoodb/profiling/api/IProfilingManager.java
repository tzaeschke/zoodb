package org.zoodb.profiling.api;

import java.util.Collection;

import org.zoodb.jdo.TransactionImpl;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public interface IProfilingManager {
	
	public void save();
	
	public IPathManager getPathManager();
	
	public IFieldManager getFieldManager();
	
	public void newTrxEvent(TransactionImpl trx);
	
	public IDataProvider getDataProvider();
	
	/**
	 * Setup profiling
	 */
	public void init();
	
	/**
	 * Finalize profiling data
	 */
	public void finish();
	
	public void addSuggestion(AbstractSuggestion s);
	
	public void addSuggestions(Collection<AbstractSuggestion> s);
	
}
