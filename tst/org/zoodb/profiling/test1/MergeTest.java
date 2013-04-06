package org.zoodb.profiling.test1;

import org.zoodb.profiling.acticvity1.AuthorMergeTest;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;

public class MergeTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(1,false, AllTest.DB_NAME);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
		actions.addAction(new AuthorMergeTest(), 1d);
	
		us.setActions(actions);
		us.run();

	}

}
