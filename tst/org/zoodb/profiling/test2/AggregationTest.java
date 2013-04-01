package org.zoodb.profiling.test2;

import org.zoodb.profiling.acticvity2.AggregationAction;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;


public class AggregationTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(1,false);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
		actions.addAction(new AggregationAction(), 1d);
		us.setActions(actions);
		us.run();

	}

}
