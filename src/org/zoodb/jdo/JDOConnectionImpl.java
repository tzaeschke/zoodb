package org.zoodb.jdo;

import javax.jdo.datastore.JDOConnection;

import org.zoodb.jdo.internal.Session;

public class JDOConnectionImpl implements JDOConnection {

	private Session _connection;
	
	public JDOConnectionImpl(Session nativeConnection) {
		_connection = nativeConnection;
	}

	public void close() {
		_connection = null;
	}

	public Object getNativeConnection() {
		return _connection;
	}
	
}
