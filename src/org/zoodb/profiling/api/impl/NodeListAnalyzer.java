package org.zoodb.profiling.api.impl;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.IListAnalyzer;
import org.zoodb.profiling.api.tree.impl.ClazzNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.suggestion.ListSuggestion;

public class NodeListAnalyzer implements IListAnalyzer {
	
	private static Logger logger = LogManager.getLogger("allLogger");
	private NodeTraverser traverser;
	private ClazzNode currentList;

	@Override
	public Collection<ListSuggestion> analyzeList(ClazzNode listTree) {
		this.currentList = listTree;
		traverser = new NodeTraverser(currentList);
		printList(listTree);
		
		Collection<ListSuggestion> listSuggestions = new ArrayList<ListSuggestion>();
		listSuggestions.add(naiveSuggestions());
		return listSuggestions;
	}

	private ListSuggestion naiveSuggestions() {
		traverser.resetAndInit(currentList);
		traverser.next();
		
		ClazzNode currentListNode = null;
		ClazzNode tailNode = null;
		while ( (currentListNode = (ClazzNode) traverser.next()) != null ) {
			if (currentListNode.isActivated()) {
				//logger.info(" to " + currentListNode.getClazz() + " #" + currentListNode.getAccessFrequency());
				tailNode = currentListNode;
			}
		}
		
		ListSuggestion ls = new ListSuggestion(currentList);
		ls.setText("Detection of a list-shaped access path from '" + currentList.getClazzName() + "' to " + tailNode.getClazzName() + ". Consider starting the path with a query on objects of type " + tailNode.getClazzName());
		
		logger.info(ls.getText());
		return ls;
	}

	private void printList(ClazzNode listTree) {
		logger.info("List from: " + listTree.getClazzName() + " #" + listTree.getObjectNodes().size());
		
		traverser.resetAndInit(currentList);
		ClazzNode currentListNode = null;
		ClazzNode tailNode = null;
		
		//first two nodes have always the same class, so skip 1st
		traverser.next();
		while ( (currentListNode = (ClazzNode) traverser.next()) != null ) {
			//unactivated nodes are childs of an activated object which access its primitive members
			if (currentListNode.isActivated()) {
				logger.info(" to " + currentListNode.getClazzName() + " #" + currentListNode.getObjectNodes().size());
				tailNode = currentListNode;
				
			}
		}
		
	}

}
