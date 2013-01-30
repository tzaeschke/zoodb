package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.zoodb.profiling.ProfilingConfig;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ClassSizeStats;
import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassSplitSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldCount;

/**
 * Calculates the cost of splitting a class into 2 attribute sets
 * @author tobiasg
 *
 */
public class SplitCostCalculator implements ICandidate {
	
	/*
	 * Counters for all possible cases that
	 * exist when splitting a class in 2 parts
	 * See documentation for a more detailed description of this cases
	 * From a cost perspective, we could merge specific cases together, however, for debugging purposes, we trace them separately
	 * (although makes code more complicated)
	 */
	int cRead1;
	int cRead2;
	int cRead3;
	
	int cWrite1;
	int cWrite2;
	int cWrite3;
	
	int cRW1;
	int cRW2;
	
	
	private ActivationArchive archive;
	private IFieldManager fm = ProfilingManager.getInstance().getFieldManager();
	
	private FieldCount[] fcs;
	private int splitIndex;
	
	private double cost;
	private double gain;
	
	/*
	 * Excepted sizes (in bytes) for the two classes
	 */
	private double sizeOfMaster;
	private double sizeOfSplittee;
	private double sizeOfOriginalMaster;
	
	private int totalActivations;
	
	private Class<?> c;
	private TrxGroup tg;
	private Set<String> trxIds;
	
	
	public SplitCostCalculator(TrxGroup tg) {
		this.tg = tg;
		
		if (tg != null) {
			trxIds = new HashSet<String>(tg.getTrxIds().size());
			//build hashset to allow for faster lookup
			trxIds.addAll(tg.getTrxIds());
		}
	}
	
	/**
	 * Calculates the cost of splitting class c in 2 parts, as indicated by the splitIndex.
	 * @param c
	 * @param fcs
	 * @param splitIndex
	 */
	public void calculateCost(Class<?> c, FieldCount[] fcs, int splitIndex) {
		this.fcs = fcs;
		this.splitIndex = splitIndex;
		this.c = c;
		
		archive = ProfilingManager.getInstance().getPathManager().getArchive(c);
		
		Iterator<AbstractActivation> archIter = archive.getIterator();
		
		totalActivations = 0;
		while(archIter.hasNext()) {
			analyzeActivation(archIter.next());
			totalActivations++;
		}
		
		//calculate the new sizes of master class and splitte class;
		sizeOfOriginalMaster = archive.getAvgObjectSize();
		sizeOfSplittee = calculateSizeOfSplittee();
		sizeOfMaster = sizeOfOriginalMaster - sizeOfSplittee + ProfilingConfig.COST_NEW_REFERENCE; 
		
		//for each activation, we have now determined to which case it belongs
		//we can now calculate the cost and gain for this split
		
		cost = totalActivations*ProfilingConfig.COST_NEW_REFERENCE;
		
		/*
		 * cRead1: both parts have been accessed in read-only mode
		 * No gain
		 * Cost: is the introduction of a new reference (this holds for all cases, will be accounted for later)
		 */
		
		/*
		 * cRead2: the master part have been accessed in read-only mode, the splitte part has not beenaccessed
		 * Gain: we did not have to load the splittee part
		 */
		gain = cRead2*sizeOfSplittee;
		
		/*
		 * cRead3: the master part has not been accessed (except the reference to splittee part (does not exist yet!!)), the splitte part has been accessed read-only
		 * No gain 
		 */
		
		/*
		 * cWrite1: both parts have been accessed in write-mode
		 * No gain
		 */
		
		/*
		 * CWrite2: master parts has been accessed in write mode, splittee part has not been accessed
		 * Gain: splittee part does not have to be written back
		 */
		gain += cWrite2*sizeOfSplittee;
		
		/*
		 * CWrite3: master part has not been accessed (only reference to splittee), splitte has been accessed in write-mode
		 * Gain: master part does not have to be written back
		 */
		gain += cWrite3*sizeOfMaster;
		
		/*
		 * cRW1: master part has been accessed in read mode, splittee part in write mode
		 * Gain: master part does not have to be written back
		 */
		gain += cRW1*sizeOfMaster;
		
		/*
		 * cRW2: master part has been accessed in write mode, splittee in read mode
		 * Gain: splittee part does not have to be written back
		 */
		gain += cRW2*sizeOfSplittee;
		
	}
	

	/**
	 * Checks the field accessed for one activation and updates the counter for the detected case
	 * @param a
	 */
	private void analyzeActivation(AbstractActivation a) {
		// if this calculator is used for a trx group, we analyze only activations for these transactions
		if (trxIds != null && !trxIds.contains(a.getTrx())) {
			return;
		}
		
		
		Collection<IFieldAccess> fas = fm.get(a.getOid(), a.getTrx());
		
		//to which case does this activation belong?
		// use a 2 flag indicator mechanism
		// 1 indicates a write, 0 indicates not accessed, and -1 indicates read only
		byte master = 0;
		byte splittee = 0;
		
		for (IFieldAccess fa : fas) {
			if (isSplitteeAttribute(fa.getFieldName())) {
				if (fa.isWrite()) {
					splittee = 1;
				} else {
					if (splittee == 0) {
						//if it is null, we do not want to change it! (a value of false indicates that there is a write in this set --> we must not forget this value!
						splittee = -1;
					}
				}
			} else {
				if (fa.isWrite()) {
					master = 1;
				} else {
					if (master == 0) {
						//same reasoning as above applies
						master = -1;
					}
				}
			}
		}
		
		//check which case we have
		if (master == -1 && splittee == -1) {
			cRead1++;
		} else if (master == -1 && splittee == 0) {
			cRead2++;
		} else if (master == 0 && splittee == -1) {
			cRead3++;
		} else if (master == 1 && splittee == 1) {
			cWrite1++;
		} else if (master == 1 && splittee == 0) {
			cWrite2++;
		} else if (master == 0 && splittee == 1) {
			cWrite3++;
		} else if (master == -1 && splittee == 1) {
			cRW1++;
		} else if (master == 1 && splittee == -1) {
			cRW2++;
		}
		
	}
	
	/**
	 * Returns true iff the attribute with namem 'fieldName' belongs to the new splittee class 
	 * @param fieldName
	 * @return
	 */
	private boolean isSplitteeAttribute(String fieldName) {
		for (int i=0;i<splitIndex;i++) {
			if (fcs[i].getName().equals(fieldName)) {
				return false;
			}
		}
		return true;
	}
	
	private double calculateSizeOfSplittee() {
		ClassSizeStats css = ProfilingManager.getInstance().getClassSizeManager().getClassStats(c);
		
		double size = 0;
		
		for (int i=0;i<splitIndex;i++) {
			size += css.getAvgFieldSizeForField(fcs[i].getName());
		}
		
		return size;
	}


	public int getcRead1() {
		return cRead1;
	}
	public int getcRead2() {
		return cRead2;
	}
	public int getcRead3() {
		return cRead3;
	}
	public int getcWrite1() {
		return cWrite1;
	}
	public int getcWrite2() {
		return cWrite2;
	}
	public int getcWrite3() {
		return cWrite3;
	}
	public int getcRW1() {
		return cRW1;
	}
	public int getcRW2() {
		return cRW2;
	}
	public double getCost() {
		return cost;
	}
	public double getGain() {
		return gain;
	}
	public double getSizeOfMaster() {
		return sizeOfMaster;
	}
	public double getSizeOfSplittee() {
		return sizeOfSplittee;
	}

	@Override
	public boolean evaluate() {
		return cost < gain;
	}

	@Override
	public double ratioEvaluate() {
		return gain / cost;
	}

	@Override
	public AbstractSuggestion toSuggestion() {
		ClassSplitSuggestion css = new ClassSplitSuggestion();
		
		//supertype attributes
		css.setClazzName(c.getName());
		css.setTotalActivations(totalActivations);
		css.setCost(cost);
		css.setGain(gain);
		css.setAvgClassSize(sizeOfOriginalMaster);
		css.setTotalWrites(ProfilingManager.getInstance().getFieldManager().getWriteCount(c));
		
		//splitt attributes
		css.setBenefitTrxs(tg.getTrxIds());
		
		css.setcRead1(cRead1);
		css.setcRead2(cRead2);
		css.setcRead3(cRead3);
		css.setcWrite1(cWrite1);
		css.setcWrite2(cWrite2);
		css.setcWrite3(cWrite3);
		css.setcRW1(cRW1);
		css.setcRW2(cRW2);
		
		css.setSizeOfMaster(sizeOfMaster);
		css.setSizeOfSplittee(sizeOfSplittee);
		
		css.setFcs(fcs);
		
		Collection<String> masterFields = new LinkedList<String>();
		for (int i=0;i<splitIndex;i++) {
			masterFields.add(fcs[i].getName());
		}
		
		
		css.setMasterFields(masterFields);
		css.setOutsourcedFields(tg.getSplittedFields());
		
		
		return css;
	}
	
	
}
