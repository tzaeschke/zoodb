package org.zoodb.profiling.api;

import java.util.Collection;

import org.zoodb.jdo.impl.TransactionImpl;
import org.zoodb.profiling.api.impl.Trx;

public interface ITrxManager {
	
	public Trx insert(String id,long start, TransactionImpl trx);
	
	public void rollback(String id);
	
	public void finish(String id,long end);
	
	public Collection<Trx> getAll(boolean includeRollbacked);

}