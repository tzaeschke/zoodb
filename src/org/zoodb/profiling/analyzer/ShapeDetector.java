package org.zoodb.profiling.analyzer;

import java.util.Collection;

import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;

/**
 * This class detects patterns in object/class trees which hold activations.
 * TODO:
 * 		- only active collections possible right now
 * 			--> extend to general reference chains 
 * @author tobiasg
 *
 */
public class ShapeDetector {
	
	/**
	 * Root node of current object/class tree 
	 */
	private AbstractNode currentTree;
	
	/**
	 * Latest node which was traversed by traverser 
	 */
	private AbstractNode currentNode;
	
	/**
	 * Latest node found by traverser which is of a candidate 
	 */
	private AbstractNode currentCandidateNode;
	
	
	private NodeTraverser<AbstractNode> traverser;
	
	
	public void init(AbstractNode rootTree){
		currentTree = rootTree;
		currentNode = currentTree;
		
		if (traverser == null) {
			traverser = new NodeTraverser<AbstractNode>(currentTree);
		} else {
			traverser.resetAndInit(currentTree);
		}
	}
	
	
	/**
	 * @return Next node in current tree which is a candidate node
	 */
	public AbstractNode detectNextCollection() {
		findNextCandidateNode();
		return currentCandidateNode;
	}
	
	
	/**
	 * A candidate node is activated and of a collection type
	 */
	private void findNextCandidateNode() {
		currentCandidateNode = null;
		while ( (currentNode = traverser.next()) != null) {
			Class<?> c = null;
			try {
				c = Class.forName(currentNode.getClazzName());
				
				if (Collection.class.isAssignableFrom(c) && currentNode.isActivated()) {
					currentCandidateNode = currentNode;
					break;
				} else {
					continue;
				}
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

}
