package org.zoodb.profiling.event;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;

import org.zoodb.jdo.QueryImpl;
import org.zoodb.jdo.TransactionImpl;

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
		try {
			Method m = null;
			switch(event) {
				case QUERY_ON_CREATE:
					m = IQueryListener.class.getMethod("onCreate", QueryImpl.class);
					break;
				case QUERY_ON_CANCEL:
					m = IQueryListener.class.getMethod("onCancel", QueryImpl.class);
					break;
				case QUERY_BEFORE_EXECUTION:
					m = IQueryListener.class.getMethod("beforeExecution", QueryImpl.class);
					break;
				case QUERY_AFTER_EXECUTION:
					m = IQueryListener.class.getMethod("afterExecution", QueryImpl.class);
					break;
			default:
				break;
			}
		
			if (m != null && queryListeners != null) {
			
				for (IQueryListener l : queryListeners) {
					m.invoke(l, query);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void fireTrxEvent(Event event,TransactionImpl trx) {
		try {
			Method m = null;
			switch(event) {
				case TRX_ON_BEGIN:
					m = ITrxListener.class.getMethod("onBegin", TransactionImpl.class);
					break;
				case TRX_BEFORE_COMMIT:
					m = ITrxListener.class.getMethod("beforeCommit", TransactionImpl.class);
					break;
				case TRX_AFTER_COMMIT:
					m = ITrxListener.class.getMethod("afterCommit", TransactionImpl.class);
					break;
				case TRX_BEFORE_ROLLBACK:
					m = ITrxListener.class.getMethod("beforeRollback", TransactionImpl.class);
					break;
			default:
				break;
			}
		
			if (m != null && trxListeners != null) {
			
				for (ITrxListener l : trxListeners) {
					m.invoke(l, trx);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			//TODO TZ Why not rethrow Exceptions???
			throw new RuntimeException(e);
		}
	}
	
	public static void register(IQueryListener listener) {
		if (queryListeners == null) {
			queryListeners = new LinkedList<IQueryListener>();
		}
		queryListeners.add(listener);
	}
	
	public static void unregister(IQueryListener listener) {
		if (queryListeners != null) {
			queryListeners.remove(listener);
		}
	}
	
	public static void register(ITrxListener listener) {
		if (trxListeners == null) {
			trxListeners = new LinkedList<ITrxListener>();
		}
		trxListeners.add(listener);
	}
	
	public static void unregister(ITrxListener listener) {
		if (trxListeners != null) {
			trxListeners.remove(listener);
		}
	}

}
