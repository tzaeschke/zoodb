package org.zoodb.profiling.api.impl;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.IPathTree;
import org.zoodb.profiling.api.IPathTreeNode;

/**
 * @author tobiasg
 *
 */
public class ListAnalyzer {

	private static Logger logger = LogManager.getLogger("allLogger");
	private TreeTraverser traverser;
	private IPathTree currentList;

	
	
	/**
	 * @param listTree
	 * @return Object/Suggestionss for this list
	 * TODO: check for query or reference optimization
	 */
	public Collection<ListSuggestion> analyzeList(IPathTree listTree) {
		this.currentList = listTree;
		traverser = new TreeTraverser(currentList);
		printList(listTree);
		
		Collection<ListSuggestion> listSuggestions = new ArrayList<ListSuggestion>();
		listSuggestions.add(naiveSuggestions());
		return listSuggestions;
	}
	
	/**
	 * Assumes all node pairs on this list have a 1:1 relationsip and no writes on attributes of intermediate nodes occur:
	 * 	--> unconditioned access: accessFrequency of each node 'x' is at least as high as its predecessor on the list
	 * 		propose a query to directly access the tail 
	 * @param listTree
	 * @return
	 */
	private ListSuggestion naiveSuggestions() {
		traverser.resetAndInit(currentList);
		traverser.next();
		
		IPathTreeNode currentListNode = null;
		IPathTreeNode tailNode = null;
		while ( (currentListNode = traverser.next()) != null ) {
			if (currentListNode.isActivatedObject()) {
				//logger.info(" to " + currentListNode.getClazz() + " #" + currentListNode.getAccessFrequency());
				tailNode = currentListNode;
			}
		}
		
		ListSuggestion ls = new ListSuggestion(currentList);
		ls.setText("Detection of a list-shaped access path from '" + currentList.getRoot().getClazz() + "' to " + tailNode.getClazz() + ". Consider starting the path with a query on objects of type " + tailNode.getClazz());
		
		logger.info(ls.getText());
		return ls;
	}
	
	/**
	 * For debugging
	 * @param listTree
	 */
	private void printList(IPathTree listTree) {
		logger.info("List from: " + listTree.getRoot().getClazz() + " #" + listTree.getRoot().getAccessFrequency());
		
		traverser.resetAndInit(currentList);
		IPathTreeNode currentListNode = null;
		IPathTreeNode tailNode = null;
		
		//first two nodes have always the same class, so skip 1st
		traverser.next();
		while ( (currentListNode = traverser.next()) != null ) {
			//unactivated nodes are childs of an activated object which access its primitive members
			if (currentListNode.isActivatedObject()) {
				logger.info(" to " + currentListNode.getClazz() + " #" + currentListNode.getAccessFrequency());
				tailNode = currentListNode;
				
			}
		}
	}
	
	

}
