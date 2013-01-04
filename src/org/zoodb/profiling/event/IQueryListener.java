package org.zoodb.profiling.event;

import org.zoodb.jdo.QueryImpl;

/**
 * Callback interface for query events.
 * Classes which implement this interface and register for query events will
 * receive notifications for the events decribed in this class.
 * @author tobiasg
 *
 */
public interface IQueryListener {
	
	/**
	 * Event fired when a new query is created.
	 * @param query
	 */
	public void onCreate(QueryImpl query);
	
	/**
	 * Event fired when a query is cancelled.
	 * @param query
	 */
	public void onCancel(QueryImpl query);
	
	/**
	 * Event fired before the query will be executed.
	 * @param query
	 */
	public void beforeExecution(QueryImpl query);
	
	/**
	 * Event fired after the query is executed.
	 * @param query
	 */
	public void afterExecution(QueryImpl query);

}