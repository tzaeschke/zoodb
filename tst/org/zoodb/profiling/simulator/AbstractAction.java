package org.zoodb.profiling.simulator;

import java.util.logging.Logger;



public abstract class AbstractAction implements IAction {

	private Logger logger = Logger.getLogger("simLogger");

	public Logger getLogger() {
		return logger;
	} 

}