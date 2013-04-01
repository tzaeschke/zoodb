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

	public ZooDBSimulator(int numberOfUsers, boolean executeConcurrent) {
		super(numberOfUsers, executeConcurrent);
	}

	@Override
	protected void init() {
		ProfilingManager.getInstance().init("dblp");
	}

	@Override
	protected void shutdown() {
		ProfilingManager.getInstance().finish();
		ProfilingManager.getInstance().save();
	}

	@Override
	protected PersistenceManagerFactory getPMF() {
		//Replace "dblp" with your database name
		ZooJdoProperties props = new ZooJdoProperties("dblp");
        return JDOHelper.getPersistenceManagerFactory(props);
	}

}
