package org.zoodb.profiling.api.impl;

import org.zoodb.jdo.TransactionImpl;
import org.zoodb.profiling.event.ITrxListener;

public class ProfilingTrxListener implements ITrxListener {
	
	private ProfilingManager mng;
	
	public ProfilingTrxListener(ProfilingManager mng) {
		this.mng = mng;
	}

	@Override
	public void onBegin(TransactionImpl trx) {
		mng.newTrxEvent(trx);
	}

	@Override
	public void afterCommit(TransactionImpl trx) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beforeRollback(TransactionImpl trx) {
		// TODO Auto-generated method stub
		
	}

}
