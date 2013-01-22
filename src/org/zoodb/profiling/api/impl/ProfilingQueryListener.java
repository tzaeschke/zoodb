package org.zoodb.profiling.api.impl;

import org.zoodb.jdo.QueryImpl;
import org.zoodb.jdo.api.impl.DBStatistics;
import org.zoodb.jdo.internal.Session;
import org.zoodb.profiling.event.IQueryListener;

public class ProfilingQueryListener implements IQueryListener {
	
	private DBStatistics dbStats;

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
		//initialize a new one for each connection --> advantage: queries could run in paralled over multiple connections and IO is still correct.
		dbStats = new DBStatistics((Session) query.getPersistenceManager().getDataStoreConnection().getNativeConnection());
		int queryId = getId(query);
		int pageCountBegin = dbStats.getStorageDataPageReadCount();
		ProfilingManager.getInstance().getQueryManager().insertProfile(queryId, query,pageCountBegin);
	}

	@Override
	public void afterExecution(QueryImpl query) {
		dbStats = new DBStatistics((Session) query.getPersistenceManager().getDataStoreConnection().getNativeConnection());
		int queryId = getId(query);
		int pageCountEnd = dbStats.getStorageDataPageReadCount();
		ProfilingManager.getInstance().getQueryManager().updateProfile(queryId, query,pageCountEnd);
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
