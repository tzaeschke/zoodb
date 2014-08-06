package org.zoodb.profiling.event;

import java.util.ArrayList;
import java.util.Collection;

import org.zoodb.jdo.impl.QueryImpl;
import org.zoodb.jdo.impl.TransactionImpl;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.tools.DBStatistics;

public class Events {
	
	public enum Event {
		//Query events
		QUERY_ON_CREATE,
		QUERY_ON_CANCEL,
		QUERY_BEFORE_EXECUTION,
		QUERY_AFTER_EXECUTION,
		
		//Transaction events,
		TRX_ON_BEGIN,
		TRX_BEFORE_COMMIT,
		TRX_AFTER_COMMIT,
		TRX_BEFORE_ROLLBACK
	}

	private static Collection<IQueryListener> queryListeners;
	private static Collection<ITrxListener> trxListeners;
	
	
	public static void fireQueryEvent(Event event,QueryImpl query) {
		if (DBStatistics.isEnabled()) {
			if (queryListeners == null) {
				trxListeners = new ArrayList<ITrxListener>();
				queryListeners = new ArrayList<IQueryListener>();
				//just in case to trigger full initialisation
				ProfilingManager.getInstance();
			}
			for (IQueryListener l : queryListeners) {
				switch(event) {
				case QUERY_ON_CREATE:
					l.onCreate(query);
					break;
				case QUERY_ON_CANCEL:
					l.onCancel(query);
					break;
				case QUERY_BEFORE_EXECUTION:
					l.beforeExecution(query);
					break;
				case QUERY_AFTER_EXECUTION:
					l.afterExecution(query);
					break;
				default:
					throw new IllegalArgumentException(event.name()); 
				}
			}
		}
	}
	
	public static void fireTrxEvent(Event event,TransactionImpl trx) {
		if (DBStatistics.isEnabled()) {
			if (trxListeners == null) {
				trxListeners = new ArrayList<ITrxListener>();
				queryListeners = new ArrayList<IQueryListener>();
				//just in case to trigger full initialisation
				ProfilingManager.getInstance();
			}
			for (ITrxListener l : trxListeners) {
				switch(event) {
				case TRX_ON_BEGIN:
					l.onBegin(trx);
					break;
				case TRX_BEFORE_COMMIT:
					l.beforeCommit(trx);
					break;
				case TRX_AFTER_COMMIT:
					l.afterCommit(trx);
					break;
				case TRX_BEFORE_ROLLBACK:
					l.beforeRollback(trx);
					break;
				default:
					throw new IllegalArgumentException(event.name()); 
				}
			}
		}
	}
	
	public static void register(IQueryListener listener) {
		if (queryListeners == null) {
			queryListeners = new ArrayList<IQueryListener>();
		}
		queryListeners.add(listener);
	}
	
	public static void unregister(IQueryListener listener) {
		queryListeners.remove(listener);
	}
	
	public static void register(ITrxListener listener) {
		if (trxListeners == null) {
			trxListeners = new ArrayList<ITrxListener>();
		}
		trxListeners.add(listener);
	}
	
	public static void unregister(ITrxListener listener) {
		trxListeners.remove(listener);
	}

}
