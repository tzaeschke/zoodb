package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.ObjectNode;
import org.zoodb.profiling.suggestion.CollectionAggregationSuggestion;

/**
 * Detects pattern:
 * 	Owner object 'y' has collection, only single element 'x' of collection is read (allow multiple?)
 * 		--> single or multiple attributes of 'x' accessed 
 * 
 *  Suggested improvement: direct reference to 'x' in 'y'
 * @author tobias
 *
 */
public class ReferenceCollectionAnalyzer {
	
	private ObjectNode currentTree;

	private Logger logger = LogManager.getLogger("allLogger");
	
	public void setObjectTree(ObjectNode on) {
		this.currentTree = on;
	}
	
	public void analyze() {
		ShapeDetector sd = new ShapeDetector();
		sd.init(currentTree);
		
		ObjectNode currentNode = null;
		while ( (currentNode = (ObjectNode) sd.detectNextCollection()) != null ) {
			
			AbstractNode activeChild = activeChildren(currentNode);
			
			if (activeChild == null) {
				continue;
			} else{
				/*
				 * Check the number of children of 'activeChild':
				 *  = 1 --> could also introduce attribute redundancy in parent of 'currentNode'
				 *  > 1 --> introduce reference in parent of 'currentNode'
				 */
				int count = activeChild.getChildren().size();
				
				Class<?> ownerClass = currentNode.getActivation().getActivator().getClass();
				long totalCollectionBytes = ((ObjectNode) activeChild).getActivation().getTotalObjectBytes();
				
				if (count == 1) {
					//TODO: handle single attribute case
				} else if (count > 1) {
					//TODO: handle attribute-set case
				} else {
					//this case is not interesting, no attributes read
				}
			}
		}
	
	}
	
	private AbstractNode activeChildren(AbstractNode parent) {
		AbstractNode activatedChild = null;
		for (AbstractNode child : parent.getChildren()) {
			if (child.isActivated()) {
				if (activatedChild == null) {
					activatedChild = child;
				} else {
					// more than 1 activated child!
					return null;
				}
			} 
		}
		return activatedChild;
		
	}
}
