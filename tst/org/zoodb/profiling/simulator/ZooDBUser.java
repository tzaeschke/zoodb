package org.zoodb.profiling.simulator;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.jdo.PersistenceManager;

import org.zoodb.internal.Session;
import org.zoodb.tools.DBStatistics;

public class ZooDBUser<V> extends User<V> {
	
	private DBStatistics dbStats;
	
	private Logger logger = Logger.getLogger("simLogger");
	
	private long pageCount = 0;

	public ZooDBUser(PersistenceManager pm, ActionArchive actions, int actionRepeat) {
		super(pm, actions, actionRepeat);
	}

	@Override
	public boolean abort() {
		boolean abort = getActionCount() >= getMaxActions();
		
		if (abort) {
			analyzeActionResults(this.getActionResults());
		}
		return abort;
	}
	
	@Override
	public void analyzeResults() {
		analyzeActionResults(this.getActionResults());
	}
	
	private void analyzeActionResults(Map<Class<?>,List<ActionResult>> results) {
		for (Class<?> actionType : results.keySet()) {
			logger.info("Analyzing Results for ActionType " + actionType.getName());
			analyzeSingleActionType(results.get(actionType));
		}
		
		//total page reads
		analyzeTotal(results);
	}
	
	private void analyzeTotal(Map<Class<?>, List<ActionResult>> results) {
		double totalTime = 0;
		long totalPages = this.pageCount;
		
		for (List<ActionResult> lar : results.values()) {
			for (ActionResult ar : lar) {
				totalTime += ar.getExecutionTime();
				System.out.println("totalTime " + totalTime + "   " + ar.getPageCount());
			}
		}
		logger.info("### TOTAL ###    Pages: " + totalPages + "    Time: " + totalTime);
	}
	
	private void analyzeSingleActionType(List<ActionResult> l) {
		long totalTime = 0;
		
		for (ActionResult ar : l) {
			logger.info("Execution time: " + ar.getExecutionTime());
			totalTime += ar.getExecutionTime();
		}
		
		double avgTimeForAction = totalTime / (double) l.size();
		
		logger.info("AvgTime for action (" + l.get(0).getActionClass().getName() + "): " + avgTimeForAction);
	}

	@Override
	public void beforeExecution() {
		dbStats = new DBStatistics((Session) this.getPm().getDataStoreConnection().getNativeConnection());
	}

	@Override
	public void afterAction(IAction action, ActionResult ar) {
		String msg = "Done with action: " + action.getClass().getName();
		msg += "    time[s]: " + ar.getExecutionTime();
		long pageIncrement = dbStats.getStorageDataPageReadCountUnique(); 
		pageCount += pageIncrement;
		msg += "    Total unique pages read: " + pageIncrement;
		logger.info(msg);
	}

}
