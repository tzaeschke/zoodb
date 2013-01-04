package org.zoodb.profiling.event;

import java.lang.reflect.Method;
import java.util.Collection;

import org.zoodb.jdo.QueryImpl;

public class QueryEventRegistry {
	
	protected static final int ON_CREATE=1;
	protected static final int ON_CANCEL=2;
	protected static final int BEFORE_EXECUTION=3;
	protected static final int AFTER_EXECUTION=4;
	
	private Collection<IQueryListener> listeners;
	
	public QueryEventRegistry() {
		
	}
	
	public void register(IQueryListener listener) {
		
	}
	
	public void unregister(IQueryListener lister) {
		
	}
	
	protected void fireEvent(int eventId,QueryImpl query) {
		try {
			Method m = null;
			switch(eventId) {
				case ON_CREATE:
					m = IQueryListener.class.getMethod("onCreate", QueryImpl.class);
				case ON_CANCEL:
					m = IQueryListener.class.getMethod("onCancel", QueryImpl.class);
				case BEFORE_EXECUTION:
					m = IQueryListener.class.getMethod("beforeExecution", QueryImpl.class);
				case AFTER_EXECUTION:
					m = IQueryListener.class.getMethod("afterExecution", QueryImpl.class);
			}
		
			if (m != null) {
			
				for (IQueryListener l : listeners) {
					m.invoke(l, query);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
