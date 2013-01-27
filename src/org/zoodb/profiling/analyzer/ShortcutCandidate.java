package org.zoodb.profiling.analyzer;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.impl.ClassSizeManager;
import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class ShortcutCandidate implements ICandidate {
	
	private Class<?> start;
	private Class<?> end;
	
	private List<PathItem> items;
	
	//fields used for evaluation
	private int totalActivationsClazz;
	private int totalWritesClazz;
	
	
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
		
		totalWritesClazz = 0;
	}

	@Override
	public boolean evaluate() {
		initFormulaTerms();
		return false;
	}

	@Override
	public double ratioEvaluate() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public AbstractSuggestion toSuggestion() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
}