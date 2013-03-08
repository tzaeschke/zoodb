package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.zoodb.jdo.api.DBCollection;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

/**
 * @author tobiasg
 * Analyzes activations of a collection and its children
 *
 */
public class CollectionAnalyzer implements IAnalyzer {
	
	private Logger logger = ProfilingManager.getProfilingLogger();

	
	@Override
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions) {
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Map<Class<?>,Long> candidates = new HashMap<Class<?>,Long>();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		AbstractActivation currentA = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
		
		//for (Class<?> c : classArchives.keySet()) {
			if (DBCollection.class.isAssignableFrom(currentArchiveClass)) {
				
				 currentArchive = ProfilingManager.getInstance().getPathManager().getArchive(currentArchiveClass);
				 
				 Iterator<AbstractActivation> iter = currentArchive.getIterator();
				 
				 while (iter.hasNext()) {
					 currentA = iter.next();
					 
					 if (currentA.getChildrenCount() == 0) {
						 Long tmp = candidates.get(currentA.getParentClass());
						 
//						 if (tmp != null) {
//							 tmp += currentA.getBytes();
//						 } else {
//							 tmp = currentA.getBytes();
//						 }
						 candidates.put(currentA.getParentClass(), tmp);
					 }
				 }
				 
				
			}
		//}
		}
		
		/*
		 * Check candidates
		 * A candidate should result in a suggestions if: 
		 *  sum(bytes_of_unused_collection_with_parent_P) > (#activations_of_P)*4
		 *  4, because by default we suggest an int (can be improved to smaller types, collection sizes are known!) 
		 */
		
		for (Class<?> c : candidates.keySet()) {
			int totalActivationsOf_c = ProfilingManager.getInstance().getPathManager().getArchive(c).size();
			long totalCollectionBytes = candidates.get(c);
			
			if (totalActivationsOf_c*4 < totalCollectionBytes) {
				
				//TODO: 2nd argument should be fieldname of collection in ownerclass
				//TODO: 4th argument should be fieldname of collection that triggered the activation (e.g. size/iterator), get via fieldmanager
				Object[] o = new Object[] {c.getName(),null,totalCollectionBytes,null};
				suggestions.add(SuggestionFactory.getUCS(o));
			}
		}
		return suggestions;
	}

}