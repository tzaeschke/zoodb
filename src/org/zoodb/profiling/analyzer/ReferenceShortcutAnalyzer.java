package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.AbstractSuggestion;

public class ReferenceShortcutAnalyzer {
	
	private Collection<ShortcutCandidate> candidates;
	
	public ReferenceShortcutAnalyzer() {
		candidates = new LinkedList<ShortcutCandidate>();
	}
	
	
	
	/**
	 * @return
	 */
	public Collection<AbstractSuggestion> analyze() {
		Collection<AbstractSuggestion> suggestions = new LinkedList<AbstractSuggestion>();
		
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		AbstractActivation currentA = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			
			Iterator<AbstractActivation> iter = currentArchive.getIterator();
			
			while (iter.hasNext()) {
				currentA = iter.next();
				
				// no paths possible from this activation when no children exist
				if (currentA.getChildrenCount() > 0) {
					//TODO: traverse and build candidates
				}
			}
		}
		
		return suggestions;
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
	private boolean evaluateChildren(AbstractActivation a) {
		Collection<IFieldAccess> fas = ProfilingManager.getInstance().getFieldManager().get(a.getOid(), a.getTrx());
		
		//TODO: only 1 of type ZooPCImpl
		return a.getChildrenCount() == 1 && fas.size() == 1;
	}
	
	
	/**
	 * Checks if the savings in bytes for a candidate is greater than 
	 * the cost of introducing a field of type reference to the starting class
	 */
	private void filterLooserCandidates() {
		
	}
}
