package org.zoodb.profiling.simulator;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.profiling.api.impl.ProfilingManager;

/**
 * Sample class for executing users against ZooDB.
 * We use this version for profiling.
 * @author tobiasg
 *
 */
public class ZooDBSimulator extends UserSimulator {

	private final String dbName;
	
	public ZooDBSimulator(int numberOfUsers, boolean executeConcurrent, String dbName) {
		super(numberOfUsers, executeConcurrent);
		this.dbName = dbName;
	}

	@Override
	protected void init() {
		ProfilingManager.getInstance().init(dbName);
	}

	@Override
	protected void shutdown() {
		ProfilingManager.getInstance().finish();
		ProfilingManager.getInstance().save();
	}

	@Override
	protected PersistenceManagerFactory getPMF() {
		ZooJdoProperties props = new ZooJdoProperties(dbName);
        return JDOHelper.getPersistenceManagerFactory(props);
	}

}
