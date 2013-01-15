package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.jdo.spi.PersistenceCapable;

import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.profiling.ProfilingConfig;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.impl.Trx;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class ClassSplitAnalyzer implements IAnalyzer {
	
	private Collection<AbstractSuggestion> suggestions;
	
	private IFieldManager fm;
	
	private Trx[] trxs;
	
	private final int MIN_FIELD_COUNT = 1;

	@Override
	public Collection<AbstractSuggestion> analzye(Collection<AbstractSuggestion> suggestions) {
		this.suggestions = suggestions;
		fm = ProfilingManager.getInstance().getFieldManager();
		
		Collection<Trx> cTransactions = ProfilingManager.getInstance().getTrxManager().getAll(false);
		int tCount = cTransactions.size();
		
		trxs = new Trx[tCount];
		trxs = cTransactions.toArray(trxs);
		
		//analyze all classes for which we have activations
		Iterator<Class<?>> iter = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		while(iter.hasNext()) {
			analyzeSingleClass(iter.next());
		}
		
		return null;
	}

	private void analyzeSingleClass(Class<?> c) {
		List<String> fields = getAllAttributes(c);
		
		if (fields.size() > MIN_FIELD_COUNT) {
			Map<String,int[]> accessVectors = buildAccessVectors(c,fields);
			
			Collection<TrxGroup> trxGroups = groupAccessVectors(accessVectors,fields);
			
			//calculate split for each group, and
			for (TrxGroup tg : trxGroups) {
				if (!tg.calculateSplit()) {
					//no split is advised for this group
					trxGroups.remove(tg);
				}
			}
			
			//go through all trxGroups and advise the split which has the best cost/gain ratio
			for (TrxGroup tg : trxGroups) {
				ActivationArchive aa = ProfilingManager.getInstance().getPathManager().getArchive(c);
				int activationCount = aa.getActivationCountByTrx(tg.getTrxIds());
				
				// cost of introducing a new reference in the splitter class 'c' (to the 'splitee' class)
				long outsourceCost = activationCount*ProfilingConfig.COST_NEW_REFERENCE;
				
			}
		}
		
	}
	
	/**
	 * Groups similar transactions together by means of how similar their field-access are on this class. 
	 * @param accessVectors
	 */
	private Collection<TrxGroup> groupAccessVectors(Map<String,int[]> accessVectors,List<String> fields) {
		Collection<TrxGroup> trxGroups = new LinkedList<TrxGroup>();
		
		SimilarityChecker sc = new SimilarityChecker(new ShapeStrategy());
		
		for (String s : accessVectors.keySet()) {
			int[] av = accessVectors.get(s);
			
			if (trxGroups.isEmpty()) {
				TrxGroup tg = new TrxGroup(fields);
				tg.addTrx(s, av);
				trxGroups.add(tg);
			} else {
				boolean groupFound = false;
				for (TrxGroup g : trxGroups) {
					if (sc.check(av, g)) {
						g.addTrx(s, av);
						groupFound = true;
						break;
					}
				}
				if (!groupFound) {
					TrxGroup tg = new TrxGroup(fields);
					tg.addTrx(s, av);
					trxGroups.add(tg);
				}
			}
		}
		
		return trxGroups;
	}
	
	/**
	 * Builds the access vectors for class c, its fields and the available transactions.
	 * An entry in an access vector corresponds to the number of field accesses for c.field in a transaction.
	 * @param c
	 * @param fields
	 * @param cTransactions
	 * @return
	 */
	private Map<String,int[]> buildAccessVectors(Class<?> c,List<String> fields) {
		int fCount = fields.size();
		int tCount = trxs.length;
		Map<String,int[]> trxAccessVectors = new HashMap<String,int[]>();
			
		int[] trxAccess = null;
		String trxId = null;
		for (int i=0;i<tCount;i++) {
			trxAccess = new int[fCount];
			trxId = trxs[i].getId();
			for (int j=0;j<fCount;j++) {
				trxAccess[j] = fm.get(c, fields.get(j), trxId );
			}
			
			//check if the trxAccess vector does not contain zeros for all fields
			if (hasEntries(trxAccess)) {
				trxAccessVectors.put(trxs[i].getId(), trxAccess);
			} 
		}
		
		return trxAccessVectors;
	}
	
	/**
	 * Returns true if the accessList for the trx has at least one field-access.
	 * @param accessList
	 * @return
	 */
	private boolean hasEntries(int[] accessList) {
		for (int i=0;i<accessList.length;i++) {
			if (accessList[i] > 0) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Returns a list with all field-names for the given class. This list includes inherited fields.
	 * @param c
	 * @return
	 */
	private List<String> getAllAttributes(Class<?> c) {
		//get all attributes of 'c' (also inherited ones)
		List<String> fNames = new Vector<String>();
		
		//only local fields
		Field[] lf = c.getDeclaredFields();
		
		//only non-transient fields
		int size = lf.length;
		for (int i=0;i<size;i++) {
			int mdf = lf[i].getModifiers();
			if (mdf == Modifier.TRANSIENT) {
				continue;
			} else {
				fNames.add(lf[i].getName());
			}
		}
		
		//inherited fields
		for (Class<?> parent = c.getSuperclass(); c!=null; parent = parent.getSuperclass()) {
			if (PersistenceCapable.class.isAssignableFrom(parent) && parent != PersistenceCapableImpl.class) {
				Field[] lfs = c.getDeclaredFields();
				int s = lfs.length;
				for (int i=0;i<s;i++) {
					int mdf = lfs[i].getModifiers();
					if (mdf == Modifier.TRANSIENT) {
						continue;
					} else {
						fNames.add(lfs[i].getName());
					}
				}
			} else {
				break;
			}

		}
		
		return fNames;
	}

}
