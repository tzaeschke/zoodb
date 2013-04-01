package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ClassSizeManager;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class ClassMergeAnalyzer implements IAnalyzer {
	
	private Collection<ClassMergeCandidate> candidates;
	
	public ClassMergeAnalyzer() {
		candidates = new LinkedList<ClassMergeCandidate>();
	}

	@Override
	public Collection<AbstractSuggestion> analyze() {
		Collection<AbstractSuggestion> suggestions = new ArrayList<AbstractSuggestion>();
		
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			currentArchive = ProfilingManager.getInstance().getPathManager().getArchive(currentArchiveClass);

			analyzeSingleArchive(currentArchive,currentArchiveClass);
		}
		
		checkCandidates(suggestions);
		
		
		return suggestions;
	}
	
	/**
	 * Evaluates all candidates 
	 */
	private void checkCandidates(Collection<AbstractSuggestion> result) {
		IPathManager pmng = ProfilingManager.getInstance().getPathManager();
		ClassSizeManager csm = ProfilingManager.getInstance().getClassSizeManager();
		for (ClassMergeCandidate cma : candidates) {
			
			//set total master activations,
			ActivationArchive masterArchive = pmng.getArchive(cma.getMaster()); 
			int masterSize = masterArchive.size();
			cma.setTotalMasterActivations(masterSize);

			//set total mergee activations
			ActivationArchive mergeeArchive = pmng.getArchive(cma.getMergee()); 
			int mergeeSize = mergeeArchive.size();
			cma.setTotalMergeeActivations(mergeeSize);
			
			//getMergeeWithoutMasterRead
			int mergeeWOMasterRead = calculateMergeeWOMasterRead(cma.getMaster(),cma.getMergee());
			cma.setMergeeWOMasterRead(mergeeWOMasterRead);
			
			cma.setSizeOfMaster(csm.getClassStats(cma.getMaster()).getAvgClassSize());
			//cma.setSizeOfMaster(masterArchive.getAvgObjectSize());
			cma.setSizeOfMergee(csm.getClassStats(cma.getMergee()).getAvgClassSize());
			//cma.setSizeOfMergee(mergeeArchive.getAvgObjectSize());
			
	
			//evaluate candidate
			if (cma.evaluate()) {
				result.add(SuggestionFactory.getCMS(cma));
			}
		}
		
	}
	
	/**
	 * Returns the number of times objects of type 'mergee' have been loaded without coming from the 'master' class.
	 * @param master
	 * @param mergee
	 * @return
	 */
	private int calculateMergeeWOMasterRead(Class<?> master, Class<?> mergee) {
		int result = 0;
		
		Iterator<AbstractActivation> mergeeIter = ProfilingManager.getInstance().getPathManager().getArchive(mergee).getIterator();
		AbstractActivation currentMergeeA = null;
		while (mergeeIter.hasNext()) {
			currentMergeeA = mergeeIter.next();
			
			if (currentMergeeA.getParent() == null) {
				result++;
			} else if (currentMergeeA.getParent().getClazz() != master) {
				result++;
			}
		}
		
		return result;
	}

	private void analyzeSingleArchive(ActivationArchive currentArchive,	Class<?> currentArchiveClass) {
		//exclude collections, because they are per definition a 1:N relationship 
		if (!Collection.class.isAssignableFrom(currentArchiveClass)) {
			
			Iterator<AbstractActivation> aaIter = currentArchive.getIterator();
			AbstractActivation current = null;
			while (aaIter.hasNext()) {
				current = aaIter.next();
				
				//activations with more than 1 children
				if (current.getChildrenCount() > 0) {
					for (AbstractActivation child : current.getChildren()) {
						Field f = evaluateChild(child,current);
						
						//check if there already exists a candidate for this (master,mergee,f) tupel
						if (f != null) {
							ClassMergeCandidate cma = getCandidate(current.getClazz(), child.getClazz(), f);
							cma.incMasterWMergeeRead();
						}
						 
					}
				}
			}
			
			
		}
		
	}

	
	
	/**
	 * Checks if this child has a 1:1 relationship
	 * @param child
	 * @param parent
	 * @return
	 */
	private Field evaluateChild(AbstractActivation child, AbstractActivation parent) {
		Field[] parentFields = parent.getClazz().getDeclaredFields();
		
		//check which fields are of mergeeType (child)
		Field matchField = null;
		int count = 0; //counts the number of matchedFields in the parent (same type as mergee)
		for (int i=0;i<parentFields.length;i++) {
			if (parentFields[i].getType() == child.getClazz()) {
				matchField = parentFields[i];
				count++;
			}
		}
		if (count != 1 ) {
			return null;
		} else {
			matchField.setAccessible(true);
			return matchField;
		}
	}
	
	private ClassMergeCandidate getCandidate(Class<?> master,Class<?> mergee,Field f) {
		ClassMergeCandidate result = null;
		
		for (ClassMergeCandidate cma : candidates) {
			if (cma.getMaster() == master
					&& cma.getMergee() == mergee
					&& cma.getField().getName().equals(f.getName()) ) {
				
				result = cma;
				break;
				
			}
		}
		
		if (result == null) {
			result = new ClassMergeCandidate(master, mergee, f);
			this.candidates.add(result);
		}
		
		return result;
	}
}
