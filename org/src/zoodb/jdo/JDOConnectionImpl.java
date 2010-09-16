package org.zoodb.jdo;

import javax.jdo.datastore.JDOConnection;

import org.zoodb.jdo.internal.Connection;

public class JDOConnectionImpl implements JDOConnection {

	private Connection _connection;
	
	public JDOConnectionImpl(Connection nativeConnection) {
		_connection = nativeConnection;
	}

	public void close() {
		_connection = null;
	}

	public Object getNativeConnection() {
		return _connection;
	}
	
}
