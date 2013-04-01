package org.zoodb.profiling.test1;

import org.zoodb.profiling.acticvity1.AggregationAction;
import org.zoodb.profiling.acticvity1.AuthorMergeTest;
import org.zoodb.profiling.acticvity1.DuplicateAction;
import org.zoodb.profiling.acticvity1.LOBTestAction;
import org.zoodb.profiling.acticvity1.ShortcutAction;
import org.zoodb.profiling.acticvity1.SplitAction;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;

public class AllTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(1,false);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
		actions.addAction(new ShortcutAction(), 0.16d);
		actions.addAction(new AuthorMergeTest(), 0.16d);
		actions.addAction(new LOBTestAction(), 0.16d);
		actions.addAction(new AggregationAction(), 0.16d);
		actions.addAction(new DuplicateAction(), 0.16d);
		actions.addAction(new SplitAction(),0.16d);
		
		us.setActions(actions);
		us.run();


	}

}
