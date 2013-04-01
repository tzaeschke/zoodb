package org.zoodb.profiling.test1;

import org.zoodb.profiling.acticvity1.Action1;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;


public class Test {

	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(1,false);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
//		actions.addAction(new AggregationTest(), 0.3d);
//		actions.addAction(new AuthorMergeTest(), 0.3d);
		actions.addAction(new Action1(),0.3d);
//		actions.addAction(new CountAllAction(),0.3d);
		
		us.setActions(actions);
		us.run();
	}

}
