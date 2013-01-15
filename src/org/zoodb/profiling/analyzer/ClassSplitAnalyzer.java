package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.jdo.spi.PersistenceCapable;

import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.impl.Trx;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class ClassSplitAnalyzer implements IAnalyzer {
	
	private Collection<AbstractSuggestion> suggestions;

	@Override
	public Collection<AbstractSuggestion> analzye(Collection<AbstractSuggestion> suggestions) {
		this.suggestions = suggestions;
		
		//analyze all classes for which we have activations
		Iterator<Class<?>> iter = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		while(iter.hasNext()) {
			analyzeSingleClass(iter.next());
		}
		
		return null;
	}

	private void analyzeSingleClass(Class<?> c) {
		//get all attributes of 'c' (also inherited ones)
		
		List<String> fields = getAllAttributes(c);
		Collection<Trx> trxs = ProfilingManager.getInstance().getTrxManager().getAll(false);
		
		Map<String,List<Integer>> trxAccessVectors = new HashMap<String,List<Integer>>();
		
		for (Trx t : trxs) {
			for (String field : fields) {
				
			}
		}
		
		
	}
	
	private List<String> getAllAttributes(Class<?> c) {
		List<String> fNames = new Vector<String>();
		
		//only local fields
		Field[] lf = c.getDeclaredFields();
		
		//only non-transient fields
		int size = lf.length;
		for (int i=0;i<size;i++) {
			int mdf = lf[i].getModifiers();
			if (mdf == Modifier.TRANSIENT) {
				continue;
			} else {
				fNames.add(lf[i].getName());
			}
		}
		
		//inherited fields
		for (Class<?> parent = c.getSuperclass(); c!=null; parent = c.getSuperclass()) {
			if (PersistenceCapable.class.isAssignableFrom(parent)) {
				Field[] lfs = c.getDeclaredFields();
				int s = lfs.length;
				for (int i=0;i<s;i++) {
					int mdf = lfs[i].getModifiers();
					if (mdf == Modifier.TRANSIENT) {
						continue;
					} else {
						fNames.add(lfs[i].getName());
					}
				}
			} else {
				break;
			}

		}
		
		return fNames;
	}

}
