package org.zoodb.profiling.api.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.jdo.TransactionImpl;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.IProfilingManager;

/**
 * @author tobiasg
 *
 */
public class ProfilingManager implements IProfilingManager {
	
	private Logger logger = LogManager.getLogger("allLogger");
	
	private static ProfilingManager singleton = null;
	
	private IPathManager pathManager;
	private IFieldManager fieldManager;
	
	
	public static ProfilingManager getInstance() {
		if (singleton == null) {
			singleton = new ProfilingManager();
		}
		return singleton;
	}
	
	private ProfilingManager() {
		pathManager = new PathManagerTreeV2();
		fieldManager = new FieldManager();
	}
	
	@Override
	public void save() {
		// TODO Auto-generated method stub
	}

	@Override
	public IPathManager getPathManager() {
		return pathManager;
	}

	@Override
	public IFieldManager getFieldManager() {
		return fieldManager;
	}

	@Override
	public void newTrxEvent(TransactionImpl trx) {
		logger.info("New Trx: " + trx.getUniqueTrxId());
	}



}
