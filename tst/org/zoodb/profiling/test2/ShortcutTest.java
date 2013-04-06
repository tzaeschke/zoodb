package org.zoodb.profiling.test2;

import org.zoodb.profiling.acticvity2.ShortcutAction;
import org.zoodb.profiling.simulator.ActionArchive;
import org.zoodb.profiling.simulator.ZooDBSimulator;

public class ShortcutTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZooDBSimulator us = new ZooDBSimulator(1,false, AllTest.DB_NAME);
		
		//build action archive
		ActionArchive actions = new ActionArchive();
		actions.addAction(new ShortcutAction(), 1d);
		us.setActions(actions);
		us.run();
	}

}
