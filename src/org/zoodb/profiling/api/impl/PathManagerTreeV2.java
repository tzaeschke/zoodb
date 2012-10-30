package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IListAnalyzer;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.ClazzNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;


public class PathManagerTreeV2 implements IPathManager {
	
	private List<ObjectNode> objectLevelTrees;
	private List<ClazzNode> classLevelTrees;
	
	private Logger logger = LogManager.getLogger("allLogger");
	
	public PathManagerTreeV2() {
		objectLevelTrees = new LinkedList<ObjectNode>();
		classLevelTrees = new LinkedList<ClazzNode>();
	}

	@Override
	public void addActivationPathNode(Activation a, Object predecessor) {
		/**
		 * predecessor == null indicates that object was returned by a query
		 */
		if (predecessor == null) {
			ObjectNode fatherNode = findNode(a.getActivator().getClass().getName(),a.getOid());
			
			if (fatherNode == null) {
				ObjectNode rootNode = new ObjectNode(a);
				rootNode.setClazzName(a.getActivator().getClass().getName());
				rootNode.setObjectId(a.getOid());
				rootNode.setTriggerName("_query");
				rootNode.setActivated(true);
				
				ObjectNode rootChild = new ObjectNode(a);
				try {
					rootChild.setClazzName(a.getMemberResult().getClass().getName());
					rootChild.setObjectId(a.getTargetOid());
					rootChild.setTriggerName(a.getMemberName());

					rootNode.addChild(rootChild);
					objectLevelTrees.add(rootNode);
				} catch(Exception e) {
					
				}
			} else {
				/**
				 * Traversal of another branch of same node.
				 */
				ObjectNode rootChild = new ObjectNode(a);
				
				try {
					rootChild.setClazzName(a.getMemberResult().getClass().getName());
					rootChild.setObjectId(a.getTargetOid());
					rootChild.setTriggerName(a.getMemberName());
					
					//it is possible that 'fatherNode' already has this child (e.g. if fatherNode is returned by the query (query root-node)
					
					if (fatherNode.hasChild(rootChild)) {
						logger.info("Same child");
					} else {
						fatherNode.addChild(rootChild);
					}
					
					//fatherNode.addChildren(rootChildren);
					
				} catch(Exception e) {
					
				}

			}
			
		} else {
			ObjectNode fatherNode = findNode(a.getActivator().getClass().getName(),a.getOid());
			fatherNode.setActivated(true);
			
			//collection fix
			if (a.getMemberResult() != null) {
				
				
				ObjectNode newChild = new ObjectNode(a);
				newChild.setClazzName(a.getMemberResult().getClass().getName());
				newChild.setObjectId(a.getTargetOid());
				newChild.setTriggerName(a.getMemberName());
				
				//it is possible that 'fatherNode' already has this child (e.g. if fatherNode is an object which is shared accross multiple objects)
				//if this is the case, then both their activations are equal
				if (fatherNode.hasChild(newChild)) {
					logger.info("Same child");
				} else {
					fatherNode.addChild(newChild);
				}
			}
		}

	}
	
	/**
	 * @param clazzName
	 * @param oid
	 * @return The first in any tree that matches (clazzName,oid)
	 */
	private ObjectNode findNode(String clazzName, String oid) {
		ObjectNode fatherNode = null;
		for (ObjectNode tree : objectLevelTrees) {
			fatherNode= tree.getNode(clazzName,oid);
			if (fatherNode != null) {
				break;
			}
		}
		return fatherNode;
	}

	@Override
	public void prettyPrintPaths() {
		for (ObjectNode rootNode : objectLevelTrees) {
			logger.info("Starting printing of new object path tree...");
			rootNode.prettyPrint(0);
		}
		for (ObjectNode rootNode : objectLevelTrees) {
			logger.info("Starting printing of new object path tree (class and trigger)...");
			rootNode.prettyPrintClassAndTrigger(0);
		}

	}

	@Override
	public void aggregateObjectPaths() {
		int pathTreeCount = objectLevelTrees.size();
		
		initClassLevelTrees();
		
		for (int i=1;i<pathTreeCount;i++) {
			overlayPathTree(objectLevelTrees.get(i));
		}

	}

	@Override
	public void prettyPrintClassPaths(boolean classLevelTrees) {
		for (ClazzNode rootNode : this.classLevelTrees) {
			logger.info("Starting printing of new class path tree...");
			rootNode.prettyPrint(0);
		}

	}

	@Override
	public void optimizeListPaths() {
		logger.info("Analyzing list paths...");
		IListAnalyzer la = new NodeListAnalyzer();
		for (ClazzNode clazzRoot : classLevelTrees) {
			if (clazzRoot.isList()) {
				logger.info("List found");
				Collection<ListSuggestion> listSuggestions = la.analyzeList(clazzRoot);
				
			}
		}

	}
	
	/**
	 * Initializes the class-level path trees with the first object level tree
	 */
	private void initClassLevelTrees() {
		NodeTraverser<ObjectNode> traverser = new NodeTraverser<ObjectNode>(objectLevelTrees.get(0));
		
		ObjectNode currentItem = null;
		ObjectNode rootObjectNode = (ObjectNode) traverser.next();
		
		ClazzNode rootClazzNode = new ClazzNode();
		rootClazzNode.setClazzName(rootObjectNode.getClazzName());
		rootClazzNode.setActivated(rootObjectNode.isActivated());
		rootClazzNode.setTriggerName(rootObjectNode.getTriggerName());
		rootClazzNode.addObjectNode(rootObjectNode);
		
		while( (currentItem = (ObjectNode) traverser.next() ) != null) {
			ClazzNode parent = rootClazzNode.getOwnerOfParent(currentItem.getParentNode());
			
			ClazzNode n = new ClazzNode();
			n.setClazzName(currentItem.getClazzName());
			n.setActivated(currentItem.isActivated());
			n.setTriggerName(currentItem.getTriggerName());
			n.addObjectNode(currentItem);
			
			//because this is the first tree, we do not need to check if the child already exists
			parent.addChild(n);
		}
		
		classLevelTrees.add(rootClazzNode);
		
	}
	
	/**
	 * @param pathTree will be integrated in the already existing class-level path trees.
	 * For each node in the pathTree, find it in one of the already existing class-level trees and insert its children nodes (children nodes only!)
	 * 
	 * 
	 * Similarity Constraint on rootNode: only classname
	 * Similarity Constraint on childNodes: classname and triggername
	 * 				--> problem if we allow multiple triggerNames
	 */
	private void overlayPathTree(ObjectNode rootNode) {
		NodeTraverser<ObjectNode> traverser = new NodeTraverser<ObjectNode>(rootNode);
		ObjectNode currentNode = null;				
		
		while ( (currentNode = (ObjectNode) traverser.next()) != null) {
			
			ClazzNode c =  getClazzNodeFromObjectNode(currentNode);
			ClazzNode matchedNode = null;
			
			for (ClazzNode rootClazzNode : classLevelTrees) {
				
				if (currentNode.getParentNode() == null) {
					//for root node, take first node with same class
					matchedNode = rootClazzNode.getNodeWithClass(c.getClazzName());
					
					if (matchedNode != null) {
						matchedNode.addObjectNode(currentNode);
					}
				} else {
					matchedNode = rootClazzNode.getOwnerOfParent(currentNode.getParentNode());
					
					if (matchedNode != null) {
						//check in which children of matchedNode we have to insert (triggerName,classname)
						boolean childFound = false;
						for (AbstractNode matchedNodeChild : matchedNode.getChildren()) {
							if (matchedNodeChild.getClazzName().equals(c.getClazzName()) && matchedNodeChild.getTriggerName().equals(c.getTriggerName()) ) {
								((ClazzNode) matchedNodeChild).addObjectNode(currentNode);
								childFound = true;
								break;
							}
						}
						
						if (!childFound) {
							//insert new 'currentNode' as a new child on 'matchedNode'
							matchedNode.addChild(c);
						}
						
						//matchedNode.addObjectNode(currentNode);
						break;
					}
				}

				
			}
			
			//check if we need to start a new class-level-tree
			if (matchedNode == null) {
				classLevelTrees.add(c);
			}
		}
	}
	
	private ClazzNode getClazzNodeFromObjectNode(ObjectNode on) {
		ClazzNode c= new ClazzNode();
		
		c.setClazzName(on.getClazzName());
		c.setActivated(on.isActivated());
		c.setTriggerName(on.getTriggerName());
		c.addObjectNode(on);
		
		return c;
	}

}
