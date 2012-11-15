package org.zoodb.profiling.analyzer;

import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;
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
	
	public void analyze() {
		if (traverser == null) {
			traverser = new NodeTraverser<ObjectNode>(currentTree);
		} else {
			traverser.resetAndInit(currentTree);
		}
		
		ObjectNode currentNode = null;
		while ( (currentNode = (ObjectNode) traverser.next()) != null ) {
			Class<?> c = null;
			try {
				c = Class.forName(currentNode.getClazzName());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//the collection root node needs to be activated for this pattern!
			if (Collection.class.isAssignableFrom(c) && currentNode.isActivated()) {
				// check all childNodes whether they have children
				boolean leafes = true;
				String triggerName = null;
				long totalBytesCount = 0;
				for (AbstractNode child : currentNode.getChildren()) {
					if (child.getChildren().isEmpty()) {
						if (triggerName == null) {
							totalBytesCount = ((ObjectNode) child).getActivation().getTotalObjectBytes();
							triggerName = child.getTriggerName();
						} else if (!triggerName.equals(child.getTriggerName()))  {
							leafes = false;
							break;
						}
						continue;
					} else {
						// at least this child is not a leaf
						leafes = false;
						break;
					}
				}
				/*
				 * every child is a leaf --> all collection items are activated but no fields read
				 * Assume user counted items --> suggest sizeAttribute in owner class
				 */
				if (leafes) {
					Class<?> activatorClass = currentNode.getActivation().getActivator().getClass();
				
					UnusedCollectionSuggestion uc = new UnusedCollectionSuggestion();
					uc.setClazz(activatorClass);
					uc.setTriggerName(triggerName);
					uc.setField(currentNode.getActivation().getField());
					uc.setTotalCollectionBytes(totalBytesCount);
					
					logger.info(uc.getText());
					
				}
				
			
				
			}
		}
	}

}
