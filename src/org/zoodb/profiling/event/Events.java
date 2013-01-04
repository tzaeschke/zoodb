package org.zoodb.profiling.event;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;

import org.zoodb.jdo.QueryImpl;

public class Events {
	
	//query events
	public static final int QUERY_ON_CREATE=1;
	public static final int QUERY_ON_CANCEL=2;
	public static final int QUERY_BEFORE_EXECUTION=3;
	public static final int QUERY_AFTER_EXECUTION=4;

	private static Collection<IQueryListener> queryListeners;
	
	
	public static void fireQueryEvent(int eventId,QueryImpl query) {
		try {
			Method m = null;
			switch(eventId) {
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
			}
		
			if (m != null) {
			
				for (IQueryListener l : queryListeners) {
					m.invoke(l, query);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
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

}
