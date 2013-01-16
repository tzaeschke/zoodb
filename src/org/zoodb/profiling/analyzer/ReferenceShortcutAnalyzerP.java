package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class ReferenceShortcutAnalyzerP implements IAnalyzer {

	private Collection<ShortcutCandidate> candidates;

	public ReferenceShortcutAnalyzerP() {
		candidates = new LinkedList<ShortcutCandidate>();
	}
	
	@Override
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions) {
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		AbstractActivation currentA = null;
		AbstractActivation currentChild = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			currentArchive = ProfilingManager.getInstance().getPathManager().getArchive(currentArchiveClass);
			
			Iterator<AbstractActivation> iter = currentArchive.getIterator();
			
			while (iter.hasNext()) {
				currentA = iter.next();
				
				// no paths possible from this activation when no children exist
				if (currentA.getChildrenCount() > 0) {
					Iterator<AbstractActivation> childIter = currentA.getChildrenIterator();
					
					//iterate over children and evaluate
					//TODO: use threadpool to speedup analysis
					while (childIter.hasNext()) {
						currentChild = childIter.next();
						
						currentChild.startEvaluation(this,currentA);	
					}
					
				} else {
					continue;
				}
			}
		}
		
		/*
		 * Check candidates
		 */
		Collection<AbstractSuggestion> newSuggestions = filterLooserCandidates();
		suggestions.addAll(newSuggestions);
		return newSuggestions;
	}
	
	
	/**
	 * Checks candidates:
	 * 	a candidate with >1 intermediate nodes needs to be checked sequentially in the same order 
	 *  where the intermediate nodes occur
	 * @return
	 */
	private Collection<AbstractSuggestion> filterLooserCandidates() {
		Collection<AbstractSuggestion> suggestions = new LinkedList<AbstractSuggestion>();
		
		for (ShortcutCandidate sc : candidates) {
			int activationCount = ProfilingManager.getInstance().getPathManager().getArchive(sc.getStart()).size();
			int numberOfIntermediates = sc.getIntermediates().size();
			
			long gain = 0;
			for (Long l : sc.getIntermediateSize()) {
				gain += l;
			}
			long cost = activationCount*17; 
			if (cost < gain) {
				Object[] o = new Object[] {sc.getStart().getName(),sc.getEnd().getName(),sc.getIntermediates(),sc.getIntermediates(),sc.getIntermediateVisitCounter()};
				suggestions.add(SuggestionFactory.getRSS(o));
			}
		}

		return suggestions;
	}

	/**
	 * Adds a candidate to the list. If the candidate already exists, it updates the intermediates.
	 * If the candidate does not yet exist, a new one will be created.
	 * @param intermediateSize 
	 * @param intermediates 
	 * @param end 
	 * @param start 
	 */
	public void putCandidate(Class<?> start, Class<?> end, List<Class<?>> intermediates, List<Long> intermediateSize) {
		for (ShortcutCandidate sc : candidates) {
			if (sc.samePath(start,end,intermediates,intermediateSize)) {
				//update
				sc.update(intermediateSize);
				//
				return;
			}
		}
		//not returned yet means no candidate found with equal path
		ShortcutCandidate sc = new ShortcutCandidate();
		sc.setEnd(end);
		sc.setStart(start);
		sc.setIntermediates(intermediates);
		sc.setIntermediateSize(intermediateSize);
		List<Integer> visitCounter = new LinkedList<Integer>();
		for (Object o : intermediates) {
			visitCounter.add(1);
		}
		sc.setIntermediateVisitCounter(visitCounter);
		
		candidates.add(sc);
	}

}