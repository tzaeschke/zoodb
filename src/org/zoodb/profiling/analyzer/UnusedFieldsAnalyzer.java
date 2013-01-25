package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import javax.jdo.spi.PersistenceCapable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.FieldManager;
import org.zoodb.profiling.api.impl.ProfilingDataProvider;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldRemovalSuggestion;

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
			
			try {
				Field[] fieldsDeclared = c.getDeclaredFields();
				
				//fieldsDeclared is an array, cannot use 'retainAll'...
				for (Field f : fieldsDeclared) {
					if ( !fieldsUsed.contains(f.getName()) ) {
						
						UnusedFieldCandidate ufc = formCandidate(c,f,null);					
						FieldRemovalSuggestion frs = SuggestionFactory.getFRS(ufc);
						
						logger.info("Found unused field: " + ufc.toString());
						suggestionsByClass.add(frs);
					}
				}
				
				//check if there are are inherited fields of class 'c'
				for (Class<?> parent = c.getSuperclass(); c!=null; parent = parent.getSuperclass()) {
					if (PersistenceCapable.class.isAssignableFrom(parent) && parent != PersistenceCapableImpl.class) {
						Field[] lfs = c.getDeclaredFields();
						int s = lfs.length;
						for (int i=0;i<s;i++) {
							int mdf = lfs[i].getModifiers();
							if (mdf == Modifier.TRANSIENT) {
								continue;
							} else {
								//we have an inherited field, check if it is unused
								String ifn = lfs[i].getName();
								if (!fieldsUsed.contains(ifn)) {
									
									UnusedFieldCandidate ufc = formCandidate(c,lfs[i],parent);
									FieldRemovalSuggestion frs = SuggestionFactory.getFRS(ufc);
									
									logger.info("Found unused field: " + ufc.toString());
									suggestionsByClass.add(frs);
								}
							}
						}
					} else {
						break;
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
	
	private UnusedFieldCandidate formCandidate(Class<?> c, Field f, Class<?> superC) {
		UnusedFieldCandidate ufc = new UnusedFieldCandidate();
		ufc.setClazz(c);
		ufc.setF(f);
		ufc.setSuperClazz(superC);
		
		ActivationArchive aa = ProfilingManager.getInstance().getPathManager().getArchive(c);
		ufc.setTotalActivationsClazz(aa.size());
		
		return ufc;
	}

}
