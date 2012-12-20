package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.jdo.api.DBCollection;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.CollectionActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.tree.impl.ObjectNode;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

/**
 * @author tobiasg
 * Analyzes activations of a collection and its children
 * If every child is accessed and has only
 *
 */
public class CollectionAggregAnalyzer {
	
	private Logger logger = LogManager.getLogger("allLogger");

	public Collection<AbstractSuggestion> analyzeAggregations() {
		Collection<AbstractSuggestion> suggestions = new LinkedList<AbstractSuggestion>();
		
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Map<Class<?>,Object[]> candidates = new HashMap<Class<?>,Object[]>();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		CollectionActivation currentA = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			
			/*
			 * The following only works when using DBCollections --> e.g. LinkedList will not be detected
			 */
			if (DBCollection.class.isAssignableFrom(currentArchiveClass)) {
				currentArchive = ProfilingManager.getInstance().getPathManager().getArchive(currentArchiveClass);
				 
				 Iterator<AbstractActivation> iter = currentArchive.getIterator();
				 
				 while (iter.hasNext()) {
					 currentA = (CollectionActivation) iter.next();
					 
					 if (currentA.getChildrenCount() == currentA.getSize()) {
						 Object[] tmpResult = isCandidate(currentA);
						 if (tmpResult != null) {
							 //do we already have a candidate for this parentClass?
							 Object[] tmp = candidates.get(currentA.getParentClass());
							 if (tmp != null) {
								 //update 
								 long b = (Long) tmp[2]; 
								 b += currentA.getBytes();
								 b += (Long) tmpResult[2]; 
								 tmp[2] = b;
								 int count = (Integer) tmp[3]; 
								 tmp[3] = count++;
								 
								 candidates.put(currentA.getParentClass(), tmp);

							 } else {
								 long b = (Long) tmpResult[2]; 
								 b += currentA.getBytes();
								 tmpResult[2] = b;
								 candidates.put(currentA.getParentClass(), tmpResult);
							 }
							 
						 }
					 }
				 }
			}
		}
		/*
		 * Check candidates
		 * A candidate should result in a suggestion if
		 * 
		 * fieldtype: type of the field which was aggregated upon
		 * 
		 * sum(bytes_of_collection_items) + sum(bytes_of_collection_of_same_type) > (#activations_of_same_type)*sizeof(fieldtype)
		 * 
		 */
		for (Class<?> c : candidates.keySet()) {
			int totalActivationsOf_c = ProfilingManager.getInstance().getPathManager().getArchive(c).size();
			Object[] candidate = candidates.get(c);
			long totalCollectionAndCollectionItemsBytes = (Long) candidate[2];
			
			//TODO: get fieldtype via reflection --> get fieldsize of fieldtype
			// use 4 as default fieldsize
			if (totalActivationsOf_c*4 < totalCollectionAndCollectionItemsBytes) {
				
				//TODO: 2nd argument should be fieldname of collection in collection owner class
				//TODO: 5th argument should be fieldtype of collection-item
				Object[] o = new Object[] {c.getName(),null,candidate[0],candidate[1],null,candidate[2],candidate[3],candidate[4]};
				suggestions.add(SuggestionFactory.getCAS(o));
			}
		}
		
		return suggestions;
	}
	
	
	/**
	 * Checks if all childs of the collection activatino 'ca' are leaves (have no children)
	 * and all access the same field
	 * @param ca
	 * @return Object[] with {className of collection item, fieldName of collection item, sum of all collectionitems byte-sizes,number_of_aggregated_items_over,read/write flag}
	 */
	private Object[] isCandidate(CollectionActivation ca) {
		Iterator<AbstractActivation> iter = ca.getChildrenIterator();
		IFieldManager fm = ProfilingManager.getInstance().getFieldManager(); 
		
		AbstractActivation current = null;
		
		String fieldName = null;
		long bytes = 0;
		Class<?> assocClass = null;
		int itemCounter = 0;
		boolean isWriteAccess = false;
		
		while (iter.hasNext()) {
			current = iter.next();
			
			//all children of 'ca' should be leaves!
			if (current.getChildrenCount() != 0) {
				return null;
			}
			
			/*
			 * get fieldAccesses of (current.oid,current.trx)
			 * --> should be exactly 1, if not, abort
			 */
			Collection<IFieldAccess> fas = fm.get(current.getOid(), current.getTrx());
			
			if (fas.size() != 1) {
				return null;
			} else {
				IFieldAccess fa = fas.iterator().next();
				if (fieldName == null) {
					//first item, initialize reference values
					fieldName = fa.getFieldName();
					assocClass = fa.getAssocClass();
					isWriteAccess = fa.isWrite();
				} else if (!fieldName.equals(fa.getFieldName())) {
					return null;
				} else {
					/*
					 * If this access has not the same access-method (read/write) as the ones before, we abort.
					 * Motivation:
					 * 	it would not make sense to suggest an aggregation if some items are accessed by read, others by write (on the same parent, same trx)!
					 */
					if (fa.isWrite() != isWriteAccess) {
						return null;
					} else {
						bytes += current.getBytes();
						itemCounter++;
					}
				}
			}
		}
		
		return new Object[] {assocClass.getName(),fieldName,bytes,itemCounter,isWriteAccess};
	}


}
