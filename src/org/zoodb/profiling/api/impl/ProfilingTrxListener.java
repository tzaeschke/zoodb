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
		mng.setCollectActivations(true);
		mng.getTrxManager().insert(trx.getUniqueTrxId(), System.currentTimeMillis());
	}

	@Override
	public void afterCommit(TransactionImpl trx) {
		mng.getTrxManager().finish(trx.getUniqueTrxId(), System.currentTimeMillis());
	}

	@Override
	public void beforeRollback(TransactionImpl trx) {
		mng.getTrxManager().rollback(trx.getUniqueTrxId());
	}

	@Override
	public void beforeCommit(TransactionImpl trx) {
		mng.setCollectActivations(false);
		
	}

}