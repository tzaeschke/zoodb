package org.zoodb.jdo.internal;

import javax.jdo.JDOFatalException;

import org.zoodb.jdo.api.Schema;
import org.zoodb.jdo.internal.client.AbstractCache;

public abstract class Connection {

	private AbstractCache _clientCache = new AbstractCache();
	private OidBuffer _oidBuffer = ZooFactory.get().createOidBuffer();
	
	public final AbstractCache getClientCache() {
		return _clientCache;
	}

	public final void newSchema(Schema schema) {
		throw new UnsupportedOperationException();
	}

	public abstract boolean isSchemaDefined(Class type);

	public void commit(boolean retainValues) {
		// TODO Auto-generated method stub
		System.err.println("STUB: Connection.commit()");
	}

	public void rollback() {
		_clientCache.rollback();
		System.err.println("STUB: Connection.rollback()");
	}
	
	public void makePersistent(Object obj) {
		System.err.println("STUB: Connection.makePersistent()");
		//allocate OID
		long oid = _oidBuffer.allocateOid();
		//add to cache
		throw new UnsupportedOperationException();
	}

	public void makeTransient(Object pc) {
		System.err.println("STUB: Connection.makeTransient()");
		//remove from cache
		throw new UnsupportedOperationException();
	}
}
