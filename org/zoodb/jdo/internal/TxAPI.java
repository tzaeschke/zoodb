package org.zoodb.jdo.internal;

import org.zoodb.jdo.api.Schema;

public interface TxAPI {

	
	void newSchema(Schema schema);

	boolean isSchemaDefined(Class type);

	void commit(boolean retainValues);

	void rollback();
	
	void makePersistent(Object obj);

	void makeTransient(Object pc);

	
}
