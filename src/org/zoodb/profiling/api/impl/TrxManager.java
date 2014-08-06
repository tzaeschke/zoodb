package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.jdo.PersistenceManager;

import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.jdo.impl.TransactionImpl;
import org.zoodb.profiling.api.ITrxManager;
import org.zoodb.schema.ZooClass;

public class TrxManager implements ITrxManager {
	
	private Map<String,Trx> trxArchive;
	
	public TrxManager() {
		trxArchive = new HashMap<String,Trx>();
	}

	@Override
	public Trx insert(String id, long start, TransactionImpl tx) {
		Trx newTrx = new Trx();
		newTrx.setId(id);
		newTrx.setStart(start);
		
		if (trxArchive.isEmpty()) {
			//first transaction: create full class list
			//This is important for the UnusedClass analysis
			PersistenceManager pm = tx.getPersistenceManager();
			Session session = ((PersistenceManagerImpl)pm).getSession();
			for (ZooClass c: session.getSchemaManager().getAllSchemata()) {
				if (c == null) {
					System.out.println("Empty class in cache....");
					continue;
				}
				Class<?> cls = c.getJavaClass();
				ZooClassDef d = ((ZooClassProxy)c).getSchemaDef();
				ProfilingManager.getInstance().getPathManager().addClass(d);
				if (cls != null) {
					ProfilingManager.getInstance().getClassSizeManager().getClassStats(cls);
				}
			}
		}
		
		trxArchive.put(id, newTrx);
		
		return newTrx;
	}

	@Override
	public void rollback(String id) {
		Trx t = trxArchive.get(id);
		t.setRollbacked(true);
	}

	@Override
	public void finish(String id,long end) {
		Trx t = trxArchive.get(id);
		if (t == null) {
			throw new IllegalStateException("id=" + id + "   end=" + end);
		}
		t.setEnd(end);
	}

	@Override
	public Collection<Trx> getAll(boolean includeRollbacked) {
		if (includeRollbacked) {
			return trxArchive.values();
		} else {
			Collection<Trx> trxs = new LinkedList<Trx>();
			
			for (Trx t : trxArchive.values()) {
				if (t.isRollbacked()) {
					continue;
				} else {
					trxs.add(t);
				}
			}
			
			return trxs;
		}
	}

}
