package org.zoodb.jdo.internal.client;

import javax.jdo.spi.StateManager;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public abstract class AbstractCache {

	public abstract boolean isSchemaDefined(Class<?> type, Node node);

	public abstract void rollback();

	public abstract void markPersistent(PersistenceCapableImpl pc, long oid, Node node);
	
	public abstract CachedObject findCO(PersistenceCapableImpl pc);
	
	public abstract void addHollow(PersistenceCapableImpl obj, Node node);
	
	public abstract void addPC(PersistenceCapableImpl obj, Node node);

	public abstract CachedObject findCoByOID(long loid);

	public abstract StateManager getStateManager();

}
