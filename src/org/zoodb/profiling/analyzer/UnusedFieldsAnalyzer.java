package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.zoodb.jdo.api.DBArrayList;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.impl.SimpleFieldAccess;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class UnusedFieldsAnalyzer implements IAnalyzer {
	
	@Override
	public Collection<AbstractSuggestion> analyze() {
		ArrayList<AbstractSuggestion> newSuggestions = new ArrayList<AbstractSuggestion>();
		
		Iterator<Class<?>> persistentClassesIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		while (persistentClassesIterator.hasNext()) {
			Class<?> currentClass = persistentClassesIterator.next();
			
			if (!DBArrayList.class.isAssignableFrom(currentClass)) {
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
		ActivationArchive archive = ProfilingManager.getInstance().getPathManager().getArchive(c);
		Iterator<AbstractActivation> archIter = archive.getIterator();
		
		ZooFieldDef[] allFields = archive.getZooClassDef().getAllFields();
		
		//a value of true indicates that this field has been accessed at leas once during the whole application
		boolean[] fieldMarks = new boolean[allFields.length];
		
		//int idx = Utils.getIndexForFieldName(fieldName, );
		
		AbstractActivation current = null;
		SimpleFieldAccess sfa = null;
		
		while (archIter.hasNext()) {
			current = archIter.next();
			//Set<Integer> fasIdx = current.getFas().keySet();
			Set<Integer> fasIdx = current.getAccessedFieldIndices();
			
			for (Integer i : fasIdx) {
				if (i > -1)	fieldMarks[i] = true;
			}
		}
		
		
		//
		//ZooClassDef clsDef = archive.getZooClassDef();
		for (int i=0;i<fieldMarks.length;i++) {
			if (!fieldMarks[i]) {
				
				//TODO: check if field is inherited
				UnusedFieldCandidate ufc = formCandidate(c,allFields[i].getJavaField(),null);
				newSuggestions.add(SuggestionFactory.getFRS(ufc));
			}
		}
	}
	
	private UnusedFieldCandidate formCandidate(Class<?> c, Field f, Class<?> superC) {
		UnusedFieldCandidate ufc = new UnusedFieldCandidate();
		ufc.setClazz(c);
		ufc.setF(f);
		ufc.setSuperClazz(superC);
		
		ActivationArchive aa = ProfilingManager.getInstance().getPathManager().getArchive(c);
		ufc.setTotalActivationsClazz(aa.size());
		int totalWritesClazz = ProfilingManager.getInstance().getFieldManager().getWriteCount(c);
		ufc.setTotalWritesClazz(totalWritesClazz);
		
		return ufc;
	}

}
