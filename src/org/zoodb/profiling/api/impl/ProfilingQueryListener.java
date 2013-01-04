package org.zoodb.profiling.api.impl;

import org.zoodb.jdo.QueryImpl;
import org.zoodb.profiling.event.IQueryListener;

public class ProfilingQueryListener implements IQueryListener {

	@Override
	public void onCreate(QueryImpl query) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onCancel(QueryImpl query) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeExecution(QueryImpl query) {
		int queryId = getId(query);
		ProfilingManager.getInstance().getQueryManager().insertProfile(queryId, query);
	}

	@Override
	public void afterExecution(QueryImpl query) {
		int queryId = getId(query);
		ProfilingManager.getInstance().getQueryManager().updateProfile(queryId, query);
	}
	
	
	private int getId(QueryImpl query) {
		int id = 0;
		
		Class<?> candidateClass = query.getCandidateClass();
		Class<?> resultClass	= query.getResultClass();
		String filter			= query.getFilter();
		boolean subClasses = query.isSubClasses();
		
		if (candidateClass != null) {
			id += candidateClass.hashCode();
		}
		if (resultClass != null) {
			id += resultClass.hashCode();
		}
		if (filter != null) {
			id += filter.hashCode();
		}
		/*
		 * TODO: include subclasses,unique
		 */
		
		return id;
	}

}
