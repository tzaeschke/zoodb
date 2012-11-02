package org.zoodb.profiling.api;

import org.zoodb.jdo.TransactionImpl;

public interface IProfilingManager {
	
	public void save();
	
	public IPathManager getPathManager();
	
	public IFieldManager getFieldManager();
	
	public void newTrxEvent(TransactionImpl trx);
	
}
