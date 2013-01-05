package org.zoodb.profiling.event;

import org.zoodb.jdo.TransactionImpl;

public interface ITrxListener {
	
	/**
	 * Fired when a new transaction begins.
	 */
	public void onBegin(final TransactionImpl trx);
	
	/**
	 * Fired when a transaction commits
	 */
	public void afterCommit(final TransactionImpl trx);
	
	/**
	 * Fired before a transaction will be reverted.
	 */
	public void beforeRollback(final TransactionImpl trx);

}
