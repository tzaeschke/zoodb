package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.zoodb.profiling.api.ITrxManager;

public class TrxManager implements ITrxManager {
	
	private Map<String,Trx> trxArchive;
	
	public TrxManager() {
		trxArchive = new HashMap<String,Trx>();
	}

	@Override
	public void insert(String id, long start) {
		Trx newTrx = new Trx();
		newTrx.setId(id);
		newTrx.setStart(start);
		
		trxArchive.put(id, newTrx);
	}

	@Override
	public void rollback(String id) {
		Trx t = trxArchive.get(id);
		t.setRollbacked(true);
	}

	@Override
	public void finish(String id,long end) {
		Trx t = trxArchive.get(id);
		t.setEnd(end);
	}

	@Override
	public Collection<Trx> getAll() {
		return trxArchive.values();
	}

}
