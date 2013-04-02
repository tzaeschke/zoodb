package org.zoodb.profiling.analyzer;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.ProfilingConfig;
import org.zoodb.profiling.api.impl.ClassSizeManager;
import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.RefPathItem;
import ch.ethz.globis.profiling.commons.suggestion.ReferenceShortcutSuggestion;

public class ShortcutCandidate implements ICandidate {
	
	private Class<?> start;
	private Class<?> end;
	
	private List<PathItem> items;
	
	//fields used for evaluation
	private int totalActivationsClazz;
	private int totalWritesClazz;
	private int totalAdditionalWrites;
	
	private double cost;
	private double gain;
	
	//end fields used for evaluation

	public ShortcutCandidate() {
		items = new LinkedList<PathItem>();
	}
	
	public Class<?> getStart() {
		return start;
	}
	public void setStart(Class<?> start) {
		this.start = start;
	}
	public Class<?> getEnd() {
		return end;
	}
	public void setEnd(Class<?> end) {
		this.end = end;
	}
	public List<PathItem> getItems() {
		return items;
	}
	public void setItems(List<PathItem> items) {
		this.items = items;
	}
	public int getTotalActivationsClazz() {
		return totalActivationsClazz;
	}
	public void setTotalActivationsClazz(int totalActivationsClazz) {
		this.totalActivationsClazz = totalActivationsClazz;
	}
	public int getTotalWritesClazz() {
		return totalWritesClazz;
	}
	public void setTotalWritesClazz(int totalWritesClazz) {
		this.totalWritesClazz = totalWritesClazz;
	}
	public int getTotalAdditionalWrites() {
		return totalAdditionalWrites;
	}
	public void setTotalAdditionalWrites(int totalAdditionalWrites) {
		this.totalAdditionalWrites = totalAdditionalWrites;
	}

	/**
	 * Checks if this candidate has the same path from start2 to end2 via items
	 * @param start2
	 * @param end2
	 * @param items
	 * @return
	 */
	public boolean samePath(Class<?> start2, Class<?> end2, List<PathItem> items) {
		boolean same = true;
		
		same &= start2 == this.start;
		same &= end2 == this.end;
		same &= items.size() == this.items.size();
		
		if (same) {
			//check if all intermediate nodes are equal
			int size = items.size();
			for (int i=0;i<size;i++) {
				same &= this.items.get(i).equals(items.get(i));
			}
		}
		
		return same;
	}

	/**
	 * Increases the visit counter for all path-items by 1 
	 */
	public void update() {
		for (PathItem pi : items) {
			pi.incVisitCounter();
		}		
	}
	
	/**
	 * Initializes all terms of the cost formula
	 */
	private void initFormulaTerms() {
		ClassSizeManager csm = ProfilingManager.getInstance().getClassSizeManager();
		
		totalActivationsClazz = ProfilingManager.getInstance().getPathManager().getArchive(start).size();
		totalWritesClazz = ProfilingManager.getInstance().getFieldManager().getWriteCount(start);
		totalAdditionalWrites = 0;
		
		for (PathItem pi : items) {
			pi.setAvgClassSize(csm.getClassStats(pi.getC()).getAvgClassSize());
			//pi.setAvgClassSize(pathMng.getArchive(start).getAvgObjectSize());
		}
		
	}

	@Override
	public boolean evaluate() {
		initFormulaTerms();
		
		cost = totalActivationsClazz*ProfilingConfig.COST_NEW_REFERENCE;
		cost += totalWritesClazz*ProfilingConfig.COST_NEW_REFERENCE;
		cost += totalAdditionalWrites*ProfilingConfig.COST_NEW_REFERENCE;
		
		
		for (PathItem pi : items) {
			gain += pi.getAvgClassSize()*pi.getTraversalCount();
		}
		
		return cost < gain;
	}

	@Override
	public double ratioEvaluate() {
		evaluate();
		return gain/cost;
	}

	@Override
	public AbstractSuggestion toSuggestion() {
		ReferenceShortcutSuggestion ss = new ReferenceShortcutSuggestion();
		
		ss.setClazzName(start.getName());
		ss.setCost(cost);
		ss.setGain(gain);
		ss.setTotalActivations(totalActivationsClazz);
		ss.setAvgClassSize(ProfilingManager.getInstance().getClassSizeManager().getClassStats(start).getAvgClassSize());
		ss.setTotalAdditionalWrites(totalAdditionalWrites);
		ss.setRefTarget(end.getName());
		
		List<RefPathItem> rpis = new LinkedList<RefPathItem>();
		RefPathItem rpi = null;
		for (PathItem pi : items) {
			rpi = new RefPathItem();
			rpi.setAvgClassSize(pi.getAvgClassSize());
			rpi.setClassName(pi.getC().getName());
			rpi.setFieldName(pi.getFieldName());
			rpi.setTraversalCount(pi.getTraversalCount());
			
			rpis.add(rpi);
		}
		ss.setItems(rpis);
		return ss;
	}
	
	
	
}