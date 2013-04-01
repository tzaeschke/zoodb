package org.zoodb.profiling;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

/**
 * Set of queries to use for DBLP model. Use these queries as a starting point for further navigation.
 */
public class DBLPQueries {
	
	private Query authorByName;
	private Query conferenceSeriesByKey;
	
	private final PersistenceManager pm;
	
	private final Class<?> authorClass;
	private final Class<?> conferenceSeriesClass;
	
	public DBLPQueries(PersistenceManager pm, Class<?> authorClass, Class<?> conferenceSeriesClass) {
		this.pm = pm;
		this.authorClass = authorClass;
		this.conferenceSeriesClass = conferenceSeriesClass;
	}

	public Query getAuthorByNameQuery() {
		if (authorByName == null) {
			authorByName = pm.newQuery(authorClass);
			authorByName.setFilter("Name == nameParam");
			authorByName.declareParameters("String nameParam");
		}
		return authorByName;
		
	}
	
	public Query getConferenceSeriesByKey() {
		if (conferenceSeriesByKey == null) {
			conferenceSeriesByKey = pm.newQuery(conferenceSeriesClass);
			conferenceSeriesByKey.setFilter("DBLPkey == keyParam");
			conferenceSeriesByKey.declareParameters("String keyParam");
		}
		return conferenceSeriesByKey;
	}

}
