package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.List;

import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;

public class CollectionAggregAnalyzer {
	
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
				// check all childNodes whether they have a single attribute read/write as children
				boolean singleAttr = true;
				
				//reference values, must hold for all grandChildren
				String triggerName = null;
				Class targetClazz = null;
				
				for (AbstractNode child : currentNode.getChildren()) {
					if (child.isActivated()) {
						List<AbstractNode> grandChildren = child.getChildren();
						
						if (grandChildren.size() == 1) {
							ObjectNode grandChild = (ObjectNode) grandChildren.get(0);
							
							String tt = grandChild.getTriggerName();
							Class tc = grandChild.getActivation().getMemberResult().getClass();
							
							
							//TODO: grandChild must not have any children for this pattern!
							if (triggerName == null && targetClazz == null) {
								//first child in list, initialize reference values
								triggerName = tt;
								targetClazz = tc;
							} else {
								if (triggerName.equals(tt) && targetClazz == tc) {
									continue;
								} else {
									singleAttr = false;
									break;
								}
							}
							
						} else {
							singleAttr = false;
							break;
						}
						
						
						continue;
					} else {
						// at least this child is not a leaf
						singleAttr = false;
						break;
					}
				}
				/*
				 * every child is a leaf --> all collection items are activated but no fields read
				 * Assume user counted items --> suggest sizeAttribute in owner class
				 */
				if (singleAttr) {
					System.out.println("leaf nodes with possible aggregation");
				}
				
			
				
			}
		}
	}


}
