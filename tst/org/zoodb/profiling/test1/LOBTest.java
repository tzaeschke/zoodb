package org.zoodb.profiling.test1;

import org.zoodb.profiling.acticvity1.LOBTestAction;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;

public class LOBTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(AllTest.N_TESTS,false, AllTest.DB_NAME);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
		actions.addAction(new LOBTestAction(), 1d);
		us.setActions(actions);
		us.run();
	}

}
