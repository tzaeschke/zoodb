package org.zoodb.profiling.test1;

import org.zoodb.profiling.ProfUtil;
import org.zoodb.profiling.acticvity1.ShortcutAction;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;

public class ShortcutTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(AllTest.N_TESTS,false, AllTest.DB_NAME);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
		actions.addAction(new ShortcutAction(), 1d);
		us.setActions(actions);
		us.run();
		
		ProfUtil.listSuggestions(ProfilingManager.getInstance().internalGetSuggestions());
	}

}
