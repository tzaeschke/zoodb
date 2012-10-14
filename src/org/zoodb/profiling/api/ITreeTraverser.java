package org.zoodb.profiling.api;

/**
 * @author tobiasg
 *
 */
public interface ITreeTraverser {
	
	/**
	 * @return returns the node in the order of traversal.
	 * Returns null if tree has been fully traversed.
	 */
	public IPathTreeNode next();
	
	/**
	 * @param newTree resets this TreeTraverser. Delete current state and initializes instance with newTree. 
	 * Upon completion, further calls to next() will return nodes of newTree.
	 */
	public void resetAndInit(IPathTree newTree);

}
