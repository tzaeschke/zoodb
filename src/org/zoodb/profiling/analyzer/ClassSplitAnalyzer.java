package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

import javax.jdo.spi.PersistenceCapable;

import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.profiling.ProfilingConfig;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.impl.Trx;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldCount;

public class ClassSplitAnalyzer implements IAnalyzer {
	
	private IFieldManager fm;
	
	private Trx[] trxs;
	
	
	
	private Logger logger = ProfilingManager.getProfilingLogger();

	@Override
	public Collection<AbstractSuggestion> analyze() {
		Collection<AbstractSuggestion> suggestions = new ArrayList<AbstractSuggestion>();
		fm = ProfilingManager.getInstance().getFieldManager();
		
		Collection<Trx> cTransactions = ProfilingManager.getInstance().getTrxManager().getAll(false);
		int tCount = cTransactions.size();
		
		trxs = new Trx[tCount];
		trxs = cTransactions.toArray(trxs);
		
		//analyze all classes for which we have activations
		Iterator<Class<?>> iter = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		while(iter.hasNext()) {
			AbstractSuggestion css = analyzeSingleClass(iter.next());
			if (css != null) {
				suggestions.add(css);
			}
		}
		
		return suggestions;
	}

	private AbstractSuggestion analyzeSingleClass(Class<?> c) {
		logger.info("Analyze " + c.getName());
		List<String> fields = getAllAttributes(c);
		
		double avgObjectSize = ProfilingManager.getInstance().getClassSizeManager().getClassStats(c).getAvgClassSize();
		
		if (fields.size() >= ProfilingConfig.SA_MIN_ATTRIBUTE_COUNT && avgObjectSize >= ProfilingConfig.SA_MIN_OBJECT_SIZE) {
			
			SplitCostCalculator writeSplit = hasWriteSplit(fields,c);
			
			if (writeSplit != null) {
				//a write split is possible, we prefer this over a possible transactional optimization
				return writeSplit.toSuggestion();
			}
			
			
			Map<String,int[]> accessVectors = buildAccessVectors(c,fields);
			
			Collection<TrxGroup> trxGroups = groupAccessVectors(accessVectors,fields,c);
			
			removeNonCriticalTrx(trxGroups);
			
			//calculate split for each group, and remove the groups which have no splits
			for (TrxGroup tg : trxGroups) {
				if (!tg.calculateSplit()) {
					//no split is advised for this group
					trxGroups.remove(tg);
				} else {
					tg.calculateSplitCost();
				}
			}
			
			//go through all trxGroups which remain and advise the split which has the best cost/gain ratio
			TrxGroup maxGainGroup = null;
			long currentMaxGain = 0;
			for (TrxGroup tg : trxGroups) {
				if (tg.getGain() > currentMaxGain) {
					currentMaxGain = tg.getGain();
					maxGainGroup = tg;
				}				
			}
			if (maxGainGroup != null) {
				return maxGainGroup.getSplitCostCalculator().toSuggestion();
			} 
		} 
		return null;
	}
	
	/**
	 * Checks if this class has a disjunct set of read and write attributes.
	 * Counts for each field the percentage of writes corresponding to loads (total activations of same class).
	 * If field is written more than x% of all loads --> would be a good candidate for outsourcing of 
	 * @param fields
	 * @param c
	 * @return
	 */
	private SplitCostCalculator hasWriteSplit(List<String> fields, Class<?> c) {
		// we use the same format as in the TrxGroups (FieldCount-containers)
		// this has the advantage that we can reuse our strategy pattern by simply providing new SplitStragy
		
		IFieldManager fm = ProfilingManager.getInstance().getFieldManager();
		
		FieldCount[] fcs = new FieldCount[fields.size()];
		FieldCount fc = null;
		
		int i=0;
		for (String fieldName : fields) {
			int totalWritesForField = fm.getRWCount(c, fieldName)[1];
			
			fc = new FieldCount(fieldName,totalWritesForField);
			fcs[i] = fc;
			i++;
		}
		
		Arrays.sort(fcs);
		
		SplitStrategyAdvisor ssa = new SplitStrategyAdvisor(new ReadWriteSplitStrategy());
		int splitIndex = ssa.checkForSplit(fcs, c); 
		
		if (splitIndex != -1) {
			//calculate gain/cost of this split
			SplitCostCalculator sca = new SplitCostCalculator(null);
			sca.calculateCost(c, fcs, splitIndex);
			return sca;
		}
		
		return null;
	}

	/**
	 * Removes transaction groups which are not critical for analysis and should not be 
	 * taken into account for the splitting algorithm. E.g. reporting/management transactions
	 * @param trxGroups
	 */
	private void removeNonCriticalTrx(Collection<TrxGroup> trxGroups) {
		//remove groups which have only a single trx
		for (TrxGroup  g : trxGroups) {
			if (g.getTrxIds().size() == 1) {
				trxGroups.remove(g);
			}
		}
	}

	/**
	 * Groups similar transactions together by means of how similar their field-access are on this class. 
	 * @param accessVectors
	 * @param c 
	 */
	private Collection<TrxGroup> groupAccessVectors(Map<String,int[]> accessVectors,List<String> fields, Class<?> c) {
		Collection<TrxGroup> trxGroups = new LinkedList<TrxGroup>();
		
		SimilarityChecker sc = new SimilarityChecker(new ShapeStrategy());
		
		for (String s : accessVectors.keySet()) {
			int[] av = accessVectors.get(s);
			
			if (trxGroups.isEmpty()) {
				TrxGroup tg = new TrxGroup(fields,c);
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
					TrxGroup tg = new TrxGroup(fields,c);
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
	 * Returns a map which maps from a trxId to an array of field access counts (an entry i in the array denotes the field access count for field c.i in this transaction)
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
