package org.zoodb.profiling.analyzer;

import java.util.Collection;

import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;

/**
 * @author tobiasg
 * Analyzes activations of a collection and its children
 *
 */
public class CollectionAnalyzer {
	
	private ObjectNode currentTree;
	private NodeTraverser traverser;
	
	public void setObjectTree(ObjectNode on) {
		this.currentTree = on;
	}
	
	public void analyze() {
		if (traverser == null) {
			traverser = new NodeTraverser(currentTree);
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
			if (Collection.class.isAssignableFrom(c)) {
				// check all childNodes whether they have children
				boolean leafes = true;
				for (AbstractNode child : currentNode.getChildren()) {
					if (child.getChildren().isEmpty()) {
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
					System.out.println("unused collection leaf nodes found");
				}
				
			
				
			}
		}
	}

}
