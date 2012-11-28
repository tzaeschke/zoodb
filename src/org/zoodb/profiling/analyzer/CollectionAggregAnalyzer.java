package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;
import org.zoodb.profiling.suggestion.AbstractSuggestion;
import org.zoodb.profiling.suggestion.CollectionAggregationSuggestion;
import org.zoodb.profiling.suggestion.SuggestionFactory;

/**
 * @author tobiasg
 * Analyzes activations of a collection and its children
 * If every child is accessed and has only
 *
 */
public class CollectionAggregAnalyzer {
	
	private ObjectNode currentTree;
	private NodeTraverser traverser;
	
	private Logger logger = LogManager.getLogger("allLogger");

	public void setObjectTree(ObjectNode on) {
		this.currentTree = on;
	}
	
	
	
//	public void analyze2() {
//		ShapeDetector sd = new ShapeDetector();
//		sd.init(currentTree);
//		
//		ObjectNode currentNode = null;
//		while ( (currentNode = (ObjectNode) sd.detectNextCollection()) != null ) {
//			// check all childNodes whether they have a single attribute read/write as children
//			boolean singleAttr = true;
//			
//			//reference values, must hold for all grandChildren
//			String triggerName = null;
//			Class<?> targetClazz = null;
//			long totalCollectionBytes = 0;
//			Field targetField = null;
//			Class<?> collectionItemClazz = null;
//			
//			for (AbstractNode child : currentNode.getChildren()) {
//				if (child.isActivated()) {
//					List<AbstractNode> grandChildren = child.getChildren();
//					
//					if (grandChildren.size() == 1) {
//						ObjectNode grandChild = (ObjectNode) grandChildren.get(0);
//						
//						totalCollectionBytes +=grandChild.getActivation().getTotalObjectBytes();
//						
//						String tt = grandChild.getTriggerName();
//						Class<?> tc = grandChild.getActivation().getMemberResultClass();
//						
//						if (triggerName == null && targetClazz == null) {
//							//first child in list, initialize reference values
//							collectionItemClazz = grandChild.getActivation().getActivatorClass();
//							targetField = grandChild.getActivation().getField();
//							triggerName = tt;
//							targetClazz = tc;
//						} else {
//							if (triggerName.equals(tt) && targetClazz == tc) {
//								continue;
//							} else {
//								singleAttr = false;
//								break;
//							}
//						}
//						
//					} else {
//						singleAttr = false;
//						break;
//					}
//					
//					
//					continue;
//				} else {
//					// at least this child is not a leaf
//					singleAttr = false;
//					break;
//				}
//			}
//			/*
//			 * every child has the same single attribute read --> all collection items are activated but no fields read
//			 * Assume user aggregated over collection --> suggest aggregationAttribute in owner class of collection
//			 */
//			if (singleAttr) {
//				Class<?> activatorClass = currentNode.getActivation().getActivatorClass();
//				
//				CollectionAggregationSuggestion ca = new CollectionAggregationSuggestion();
//				//ca.setClazz(activatorClass);
//				//ca.setCollectionItem(collectionItemClazz);
//				ca.setTriggerName(triggerName);
//				ca.setTotalCollectionBytes(totalCollectionBytes);
//				ca.setField(targetField);
//				//ca.setOwnerCollectionField(currentNode.getActivation().getField());
//				
//				logger.info(ca.getText());
//				System.out.println("leaf nodes with possible aggregation");
//			}
//			
//		
//			
//		}
//	}
	
	public Collection<AbstractSuggestion> analyzeAggregations() {
		Collection<AbstractSuggestion> suggestions = new LinkedList<AbstractSuggestion>();
		
		Iterator<Class<?>> archiveIterator = ProfilingManager.getInstance().getPathManager().getClassIterator();
		
		Map<Class<?>,Object[]> candidates = new HashMap<Class<?>,Object[]>();
		
		Class<?> currentArchiveClass = null;
		ActivationArchive currentArchive = null;
		CollectionActivation currentA = null;
		
		while (archiveIterator.hasNext()) {
			currentArchiveClass = archiveIterator.next();
			
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
				Object[] o = new Object[] {c.getName(),null,candidate[0],candidate[1],null,candidate[2]};
				suggestions.add(SuggestionFactory.getCAS(o));
			}
		}
		
		return suggestions;
	}
	
	
	/**
	 * Checks if all childs of the collection activatino 'ca' are leaves (have no children)
	 * and all access the same field
	 * @param ca
	 * @return Object[] with {className of collection item, fieldName of collection item, sum of all collectionitems byte-sizes}
	 */
	private Object[] isCandidate(CollectionActivation ca) {
		Iterator<AbstractActivation> iter = ca.getChildrenIterator();
		IFieldManager fm = ProfilingManager.getInstance().getFieldManager(); 
		
		AbstractActivation current = null;
		
		String fieldName = null;
		long bytes = 0;
		Class<?> assocClass = null;
		
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
					fieldName = fa.getFieldName();
					assocClass = fa.getAssocClass();
				} else if (!fieldName.equals(fa.getFieldName())) {
					return null;
				} else {
					bytes += current.getBytes();
				}
				
			}
		}
		
		return new Object[] {assocClass.getName(),fieldName,bytes};
	}


}
