package org.zoodb.profiling.simulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


import javax.jdo.PersistenceManager;



public abstract class User<V> implements Callable<V> {
	
	private final PersistenceManager pm;
	
	private final int maxActions;
	private int actionCount = 0;
	
	private final ActionArchive actions;
	private final Map<Class<?>,List<ActionResult>> actionResults = 
			new HashMap<Class<?>,List<ActionResult>>();
	
	public User(PersistenceManager pm, ActionArchive actions, int actionRepeat) {
		this.pm = pm;
		this.actions = actions;
		this.maxActions = actionRepeat;
	}

	public V call() throws Exception { 
		
		beforeExecution();
	
//		IAction currentAction;
//		while (!abort()) {
//			long start = System.currentTimeMillis();
//			currentAction = actions.getNextAction();
//			Object result = currentAction.executeAction(pm);
//			long end = System.currentTimeMillis();
//			
//			//do some bookkeeping about executed actions
//			ActionResult ar = new ActionResult();
//			ar.setActionClass(currentAction.getClass());
//			ar.setExecutionTime(end-start);
//			addActionResult(ar);
//			
//			afterAction(currentAction,ar);
//			
//			
//			actionCount++;
//		}
		for (IAction currentAction: actions.getAllActions()) {
			long start = System.currentTimeMillis();
			Object result = currentAction.executeAction(pm);
			long end = System.currentTimeMillis();
			
			//do some bookkeeping about executed actions
			ActionResult ar = new ActionResult();
			ar.setActionClass(currentAction.getClass());
			ar.setExecutionTime(end-start);
			addActionResult(ar);
			
			afterAction(currentAction,ar);
			
			
			actionCount++;
		}
	
		analyzeResults();
		
		return null;
	}
	
	public abstract void analyzeResults();

	private void addActionResult(ActionResult ar) {
		List<ActionResult> resultForClass = actionResults.get(ar.getActionClass());
		
		if (resultForClass == null) {
			resultForClass = new LinkedList<ActionResult>();
			actionResults.put(ar.getActionClass(), resultForClass);
		}
		resultForClass.add(ar);
	}
	
	/**
	 * Checks if the user can abort. 
	 * @return
	 */
	public abstract boolean abort();
	
	public abstract void beforeExecution();
	
	public abstract void afterAction(IAction action, ActionResult ar);

	public int getMaxActions() {
		return maxActions;
	}
	public PersistenceManager getPm() {
		return pm;
	}
	public int getActionCount() {
		return actionCount;
	}
	public Map<Class<?>,List<ActionResult>> getActionResults() {
		return actionResults;
	}

}