package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;
import org.zoodb.profiling.suggestion.CollectionAggregationSuggestion;

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
			if (Collection.class.isAssignableFrom(c) && currentNode.isActivated()) {
				// check all childNodes whether they have a single attribute read/write as children
				boolean singleAttr = true;
				
				//reference values, must hold for all grandChildren
				String triggerName = null;
				Class<?> targetClazz = null;
				long totalCollectionBytes = 0;
				Field targetField = null;
				Class<?> collectionItemClazz = null;
				
				for (AbstractNode child : currentNode.getChildren()) {
					if (child.isActivated()) {
						List<AbstractNode> grandChildren = child.getChildren();
						
						
						if (grandChildren.size() == 1) {
							ObjectNode grandChild = (ObjectNode) grandChildren.get(0);
							
							totalCollectionBytes +=grandChild.getActivation().getTotalObjectBytes();
							
							String tt = grandChild.getTriggerName();
							Class<?> tc = grandChild.getActivation().getMemberResult().getClass();
							
							if (triggerName == null && targetClazz == null) {
								//first child in list, initialize reference values
								collectionItemClazz = grandChild.getActivation().getActivator().getClass();
								targetField = grandChild.getActivation().getField();
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
				 * every child has the same single attribute read --> all collection items are activated but no fields read
				 * Assume user aggregated over collection --> suggest aggregationAttribute in owner class of collection
				 */
				if (singleAttr) {
					Class<?> activatorClass = currentNode.getActivation().getActivator().getClass();
					
					CollectionAggregationSuggestion ca = new CollectionAggregationSuggestion();
					ca.setClazz(activatorClass);
					ca.setCollectionItem(collectionItemClazz);
					ca.setTriggerName(triggerName);
					ca.setTotalCollectionBytes(totalCollectionBytes);
					ca.setField(targetField);
					ca.setOwnerCollectionField(currentNode.getActivation().getField());
					
					logger.info(ca.getText());
					System.out.println("leaf nodes with possible aggregation");
				}
				
			
				
			}
		}
	}


}
