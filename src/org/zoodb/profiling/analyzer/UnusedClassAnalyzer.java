package org.zoodb.profiling.analyzer;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.zoodb.api.DBCollection;
import org.zoodb.profiling.api.impl.ClassSizeStats;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class UnusedClassAnalyzer implements IAnalyzer {
	
	@Override
	public Collection<AbstractSuggestion> analyze() {
		ArrayList<AbstractSuggestion> newSuggestions = new ArrayList<AbstractSuggestion>();
		
		Iterator<Class<?>> persistentClassesIterator = 
				ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		while (persistentClassesIterator.hasNext()) {
			Class<?> currentClass = persistentClassesIterator.next();
			
			if (!DBCollection.class.isAssignableFrom(currentClass)) {
				analyzeSingleClass(currentClass, newSuggestions);
			}
		}
		
		return newSuggestions;
	}
	
	/**
	 * Reports fields accessed by any object of class 'clazzName'
	 * @param clazzName
	 * @return
	 */
	private void analyzeSingleClass(Class<?> c, Collection<AbstractSuggestion> newSuggestions) {
		ClassSizeStats s = ProfilingManager.getInstance().getClassSizeManager().getClassStats(c);
		if (s.getTotalDeserializations() == 0) {
			if (!Modifier.isAbstract(c.getModifiers())) {
				String pkg = c.getName();
				if (pkg.startsWith("org.zoodb.api") || pkg.startsWith("org.zoodb.jdo")) {
					//skip internal classes
					return;
				}
				//We do not check whether this class has only one (or none) sub-class, because
				//that is the task of the static analyser.
				UnusedClassCandidate ucc = formCandidate(c);
				newSuggestions.add(SuggestionFactory.getCRS(ucc));
			}
		}
	}
	
	private UnusedClassCandidate formCandidate(Class<?> c) {
		UnusedClassCandidate ucc = new UnusedClassCandidate();
		ucc.setClazz(c);
		
		return ucc;
	}

}
