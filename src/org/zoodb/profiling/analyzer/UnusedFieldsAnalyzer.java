package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.impl.FieldManager;
import org.zoodb.profiling.api.impl.ProfilingDataProvider;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldRemovalSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldSuggestion;

public class UnusedFieldsAnalyzer implements IAnalyzer {
	
	private ProfilingDataProvider dp;
	
	private Logger logger = ProfilingManager.getProfilingLogger();
	
	public UnusedFieldsAnalyzer() {
		dp = new ProfilingDataProvider();
    	dp.setFieldManager((FieldManager) ProfilingManager.getInstance().getFieldManager());
	}

	@Override
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions) {
		Collection<AbstractSuggestion> suggestionsByClass = new LinkedList<AbstractSuggestion>();
		for (Class<?> c : dp.getClasses()) {
			
			Collection<String> fieldsUsed = getFieldsUsedByClass(c);
			FieldSuggestion  fs = null;
			
			try {
				Field[] fieldsDeclared = c.getDeclaredFields();
				
				//fieldsDeclared is an array, cannot use 'retainAll'...
				for (Field f : fieldsDeclared) {
					if ( !fieldsUsed.contains(f.getName()) ) {
						
						FieldRemovalSuggestion frs = SuggestionFactory.getFRS(new Object[] {c.getName(), f.getName()});
						
						//fs.setFieldAccesses(dp.getByClassAndField(c, f.getName()));
						logger.info(frs.getText());
						suggestionsByClass.add(frs);
					}
				}
				
			} catch (SecurityException e) {
				e.printStackTrace();
			} 
			
		}
		suggestions.addAll(suggestionsByClass);
		return suggestionsByClass;
	}
	
	/**
	 * Reports fields accessed by any object of class 'clazzName'
	 * @param clazzName
	 * @return
	 */
	private Collection<String> getFieldsUsedByClass(Class<?> c) {
		Set<IFieldAccess> allAccesses =dp.getByClass(c);
		
		Collection<String> usedFields = new HashSet<String>();
		
		for (IFieldAccess ac : allAccesses) {
			if (ac.isActive()) {
				usedFields.add(ac.getFieldName());
			}
		}
		return usedFields;
	}

}
