package org.zoodb.profiling.api.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.IPathTree;
import org.zoodb.profiling.api.IPathTreeNode;

public class ListAnalyzer {

	private static Logger logger = LogManager.getLogger("allLogger");

	/**
	 * @param listTree
	 * @return Object/Suggestion for this list
	 */
	public static Object analyzeList(IPathTree listTree) {
		logger.info("List from: " + listTree.getRoot().getClazz() + " #" + listTree.getRoot().getAccessFrequency());
		TreeTraverser tt = new TreeTraverser(listTree);
		IPathTreeNode currentListNode = null;
		
		//first two nodes have always the same class, so skip 1st
		tt.next();
		while ( (currentListNode = tt.next()) != null ) {
			
			if (currentListNode.isActivatedObject()) {
				logger.info(" to " + currentListNode.getClazz() + " #" + currentListNode.getAccessFrequency());
			}
		}
		return null;
		
	}

}
