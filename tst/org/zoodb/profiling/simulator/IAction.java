package org.zoodb.profiling.simulator;

import javax.jdo.PersistenceManager;

public interface IAction {
	
	public Object executeAction(PersistenceManager pm);

}
