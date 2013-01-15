package org.zoodb.profiling.api;

import java.util.Collection;

import org.zoodb.profiling.api.impl.Trx;

public interface ITrxManager {
	
	public void insert(String id,long start);
	
	public void rollback(String id);
	
	public void finish(String id,long end);
	
	public Collection<Trx> getAll(boolean includeRollbacked);

}