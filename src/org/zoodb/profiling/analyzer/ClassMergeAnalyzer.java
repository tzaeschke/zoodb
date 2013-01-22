package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class ClassMergeAnalyzer implements IAnalyzer {
	
	private Collection<ClassMergeCandidate> candidates;
	private Collection<AbstractSuggestion> result;
	
	private Logger logger = ProfilingManager.getProfilingLogger();
	
	public ClassMergeAnalyzer() {
		candidates = new LinkedList<ClassMergeCandidate>();
	}

	@Override
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions) {
		result = new LinkedList<AbstractSuggestion>();
		
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		AbstractActivation currentA = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			currentArchive = ProfilingManager.getInstance().getPathManager().getArchive(currentArchiveClass);

			analyzeSingleArchive(currentArchive,currentArchiveClass);
		}
		
		checkCandidates();
		
		
		suggestions.addAll(result);
		return result;
	}
	
	/**
	 * Evaluates all candidates 
	 */
	private void checkCandidates() {
		IPathManager pmng = ProfilingManager.getInstance().getPathManager();
		for (ClassMergeCandidate cma : candidates) {
			
			//set total master activations,
			int masterSize = pmng.getArchive(cma.getMaster()).size();
			cma.setTotalMasterActivations(masterSize);

			//set total mergee activations
			int mergeeSize = pmng.getArchive(cma.getMergee()).size();
			cma.setTotalMergeeActivations(mergeeSize);
			
			//getMergeeWithoutMasterRead
			int mergeeWOMasterRead = calculateMergeeWOMasterRead(cma.getMaster(),cma.getMergee());
			cma.setMergeeWOMasterRead(mergeeWOMasterRead);
			
			//calculate epsilon
			cma.calculateEpsilon();
			
			//evaluate candidate
			if (cma.evaluate()) {
				//create suggestion
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

	
	
	/**
	 * Checks if this child has a 1:1 relationship
	 * @param child
	 * @param parent
	 * @return
	 */
	private Field evaluateChild(AbstractActivation child, AbstractActivation parent) {
		Field f = child.getParentField();
		
		if (f != null) {
			if ( !Collection.class.isAssignableFrom(f.getType()) && !f.getType().isArray() ) {
				return f;
			}
		} else {
			logger.error("ParentField is null for child: " + child.toString());
		}
		
		return null;
	}
	
	private ClassMergeCandidate getCandidate(Class<?> master,Class<?> mergee,Field f) {
		ClassMergeCandidate result = null;
		
		for (ClassMergeCandidate cma : candidates) {
			if (cma.getMaster() == master
					&& cma.getMergee() == mergee
					&& cma.getField() == f) {
				
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
