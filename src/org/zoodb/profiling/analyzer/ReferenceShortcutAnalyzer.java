package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.impl.SimpleFieldAccess;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class ReferenceShortcutAnalyzer {
	
	private Collection<ShortcutCandidate> candidates;
	
	public ReferenceShortcutAnalyzer() {
		candidates = new LinkedList<ShortcutCandidate>();
	}
	
	
	
	/**
	 * @return
	 */
	public Collection<AbstractSuggestion> analyze() {
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
					while (childIter.hasNext()) {
						currentChild = childIter.next();
						
						if (evaluateChild(currentChild)) {
							AbstractActivation grandChild = currentChild.getChildrenIterator().next();
							/*
							 * we know: currentChild has exactly one child
							 * --> check if the candidate from currentArchive.class to grandchild.class already exists  
							 */
							ShortcutCandidate c = hasCandidate(currentA.getClazz(),grandChild.getClazz());
							
							if (c == null) {
								c = new ShortcutCandidate();
								c.setStart(currentA.getClazz());
								c.setEnd(grandChild.getClazz());
								//c.addIntermediate(currentChild.getClazz(), currentChild.getBytes());
								candidates.add(c);
							} else {
								//c.updateIntermediate(currentChild.getClazz(),currentChild.getBytes());
							}
						}
					}
					
					
			
				}
			}
		}
		/*
		 * Check candidates
		 */
		return filterLooserCandidates();
	}
	
	
	/**
	 * Checks if 'a' has a single child and no other fieldAccesses
	 * 
	 * Problem: if a has no other fieldaccesses, it would not make sense to load it in the first place
	 * --> a needs at least one fieldAcess, but at most one on an attribute of type ZooPCImpl
	 * TODO: enhance activation to include fieldname (fieldtype can be omitted, but then we need reflection to find its type)
	 * 
	 * One could also check if a's child has a single child (is that a problem if one checks only if a reference is non-null?!)
	 * 
	 * @param a
	 * @return
	 */
	private boolean evaluateChild(AbstractActivation a) {
		//Collection<SimpleFieldAccess> fas = ProfilingManager.getInstance().getFieldManager().get(a); 
		Collection<SimpleFieldAccess> fas = a.getFas2();
		
		//TODO: only 1 of type ZooPCImpl
		return a.getChildrenCount() == 1 && fas.size() == 1;
	}
	
	
	/**
	 * Checks if the savings in bytes for a candidate is greater than 
	 * the cost of introducing a field of type reference to the starting class
	 * 
	 * cost = (#activations of parent-class)*sizeof(new_reference)
	 * gain = sum of all bytes in the intermediary nodes
	 * @return 
	 * 
	 */
	private Collection<AbstractSuggestion> filterLooserCandidates() {
		Collection<AbstractSuggestion> suggestions = new LinkedList<AbstractSuggestion>();
		
		for (ShortcutCandidate c : candidates) {
			int activationCount = ProfilingManager.getInstance().getPathManager().getArchive(c.getStart()).size();
			
			long gain = 0;
			//for (Long l : c.getIntermediateSize()) {
			//	gain += l;
			//}
			
//			if (activationCount*17 < gain) {
//				//Object[] o = new Object[] {c.getStart().getName(),c.getEnd().getName(),c.getIntermediates(),c.getIntermediates(),c.getIntermediateVisitCounter()};
//				suggestions.add(SuggestionFactory.getRSS(o));
//			}
		}
		return suggestions;
	}
	
	/**
	 * Checks if there already exists a candidate with the same (start,end) pair
	 * @param start
	 * @param end
	 * @return
	 */
	private ShortcutCandidate hasCandidate(Class<?> start, Class<?> end) {
		for (ShortcutCandidate c : candidates) {
			if (c.getStart() == start && c.getEnd() == end) {
				return c;
			}
		}
		return null;		
	}
}
