package org.zoodb.profiling.test2;

import org.zoodb.profiling.acticvity2.DuplicateAction;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;

public class DuplicateTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(AllTest.N_TESTS,false, AllTest.DB_NAME);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
		actions.addAction(new DuplicateAction(), 1d);
		us.setActions(actions);
		us.run();

	}

}
