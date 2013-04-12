package org.zoodb.profiling.test2;

import org.zoodb.profiling.ProfUtil;
import org.zoodb.profiling.acticvity2.MistakeAction;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;


public class Test {

	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(1,false, AllTest.DB_NAME);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
//		actions.addAction(new AggregationTest(), 0.3d);
//		actions.addAction(new AuthorMergeTest(), 0.3d);
//		actions.addAction(new Action1(),0.3d);
//		actions.addAction(new CountAllAction(),0.3d);
		actions.addAction(new MistakeAction(),0.3d);
		
		us.setActions(actions);
		us.run();
		
		ProfUtil.listSuggestions(ProfilingManager.getInstance().internalGetSuggestions());
	}

}
