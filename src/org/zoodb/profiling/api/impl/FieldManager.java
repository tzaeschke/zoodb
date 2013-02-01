package org.zoodb.profiling.api.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.Utils;


/**
 * @author tobiasg
 *
 */
public class FieldManager implements IFieldManager {
	
	private Map<Class<?>,LobDetectionArchive> lobCandidates;
	
	private Logger logger = ProfilingManager.getProfilingLogger();
	
	public FieldManager() {
		lobCandidates = new HashMap<Class<?>,LobDetectionArchive>();
	}
	
	
	@Override
	public void insertFieldAccess(IFieldAccess fa) {}

	public Map<String, IFieldAccess> getFieldAccesses() {
		return null;
	}

	
	@Override
	public void updateLobCandidates(Class<?> clazz, Field f) {
		LobDetectionArchive lc = lobCandidates.get(clazz);
		
		if (lc == null) {
			lc = new LobDetectionArchive(clazz);
		}
		lc.incDetectionCount(f);
		lobCandidates.put(clazz, lc);
	}
	
	private int getCountByClassField(Class<?> c,String fieldName,String trxId) {
		ActivationArchive archive = ProfilingManager.getInstance().getPathManager().getArchive(c);
		Iterator<AbstractActivation> archIter = archive.getIterator();
		int idx = Utils.getIndexForFieldName(fieldName, archive.getZooClassDef());
		
		AbstractActivation current = null;
		SimpleFieldAccess sfa = null;
		
		int count = 0;
		
		while (archIter.hasNext()) {
			current = archIter.next();
			sfa = current.getFas().get(idx);
			
			if (trxId == null || ((trxId != null) && trxId.equals(current.getTrx()))) {
				if (sfa != null) {
					count += sfa.getrCount();
					count += sfa.getwCount();
				}
			} 
		}
		return count;
	}


	@Override
	public int get(Class<?> c, String field, String trx) {
		return getCountByClassField(c,field,trx);
	}
	
	@Override
	public Collection<LobDetectionArchive> getLOBCandidates() {
		return lobCandidates.values();
	}


	@Override
	public int[] getRWCount(Class<?> c, String fieldName) {
		int[] count = new int[2];
		
		ActivationArchive archive = ProfilingManager.getInstance().getPathManager().getArchive(c);
		Iterator<AbstractActivation> archIter = archive.getIterator();
		int idx = Utils.getIndexForFieldName(fieldName, archive.getZooClassDef());
		
		AbstractActivation current = null;
		SimpleFieldAccess sfa = null;
		int writeCount = 0;
		int readCount =0;
		while (archIter.hasNext()) {
			current = archIter.next();
			
			sfa = current.getFas().get(idx);
			
			if (sfa != null) {
				readCount += sfa.getrCount();
				writeCount += sfa.getwCount();
			}
		}
		count[0] = readCount;
		count[0] = writeCount;
		return count;
	}


	@Override
	public int getWriteCount(Class<?> c) {
		Iterator<AbstractActivation> archIter = ProfilingManager.getInstance().getPathManager().getArchive(c).getIterator();
		
		AbstractActivation current = null;
		int writeCount = 0;
		while (archIter.hasNext()) {
			current = archIter.next();
			if (current.hasWriteAccess()) {
				writeCount++;
			}
		}
		return writeCount;
	}


	@Override
	public Collection<SimpleFieldAccess> get(AbstractActivation a) {
		return a.getFas().values();
	}
	

}