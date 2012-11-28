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
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;
import org.zoodb.profiling.suggestion.AbstractSuggestion;
import org.zoodb.profiling.suggestion.UnusedCollectionSuggestion;

/**
 * @author tobiasg
 * Analyzes activations of a collection and its children
 *
 */
public class CollectionAnalyzer {
	
	private ObjectNode currentTree;
	private NodeTraverser<ObjectNode> traverser;
	
	private Logger logger = LogManager.getLogger("allLogger");

	
	public void setObjectTree(ObjectNode on) {
		this.currentTree = on;
	}
	
//	public void analyze() {
//		if (traverser == null) {
//			traverser = new NodeTraverser<ObjectNode>(currentTree);
//		} else {
//			traverser.resetAndInit(currentTree);
//		}
//		
//		ObjectNode currentNode = null;
//		while ( (currentNode = (ObjectNode) traverser.next()) != null ) {
//			Class<?> c = null;
//			try {
//				c = Class.forName(currentNode.getClazzName());
//			} catch (ClassNotFoundException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			//the collection root node needs to be activated for this pattern!
//			if (Collection.class.isAssignableFrom(c) && currentNode.isActivated()) {
//				// check all childNodes whether they have children
//				boolean leafes = true;
//				String triggerName = null;
//				long totalBytesCount = 0;
//				for (AbstractNode child : currentNode.getChildren()) {
//					if (child.getChildren().isEmpty()) {
//						if (triggerName == null) {
//							totalBytesCount = ((ObjectNode) child).getActivation().getTotalObjectBytes();
//							triggerName = child.getTriggerName();
//						} else if (!triggerName.equals(child.getTriggerName()))  {
//							leafes = false;
//							break;
//						}
//						continue;
//					} else {
//						// at least this child is not a leaf
//						leafes = false;
//						break;
//					}
//				}
//				/*
//				 * every child is a leaf --> all collection items are activated but no fields read
//				 * Assume user counted items --> suggest sizeAttribute in owner class
//				 */
//				if (leafes) {
//					Class<?> activatorClass = currentNode.getActivation().getActivatorClass();
//				
//					UnusedCollectionSuggestion uc = new UnusedCollectionSuggestion();
//					//uc.setClazz(activatorClass);
//					//uc.setTriggerName(triggerName);
//					uc.setField(currentNode.getActivation().getField());
//					uc.setTotalCollectionBytes(totalBytesCount);
//					
//					logger.info(uc.getText());
//					
//				}
//				
//			
//				
//			}
//		}
//	}
	
	public Collection<AbstractSuggestion> analyzeUnused() {
		Collection<AbstractSuggestion> suggestions = new LinkedList<AbstractSuggestion>();
		
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
						 
						 if (tmp != null) {
							 tmp += currentA.getBytes();
						 } else {
							 tmp = currentA.getBytes();
						 }
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
			
			if (totalActivationsOf_c*4 < candidates.get(c)) {
				//TODO: factory pattern for suggestions?
				System.out.println("unused collection found");
			}
		}
		return suggestions;
	}
	
	
	

}
