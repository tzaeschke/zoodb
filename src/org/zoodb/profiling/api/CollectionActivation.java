package org.zoodb.profiling.api;


public class CollectionActivation extends AbstractActivation {

	
	/**
	 * Size of this collection
	 * Handy for aggregation suggestions, otherwise we don't know if we aggregated over _all_ its children
	 *  
	 */
	private int size;
	
	/**
	 *  Whether this collection was accessed (at least once) (necessary for 'unused collection' suggestion)
	 *  It is easier to have this boolean here rather than check whether it has children in all subclasses
	 *  To find the subclass, we need to know the generic type of the collection 
	 *  (otherwise it is very costly to look up its children, becasuse we don't know in which class to search for)
	 *  
	 */
	private boolean accessed;

	
	
	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public boolean isAccessed() {
		return accessed;
	}

	public void setAccessed(boolean accessed) {
		this.accessed = accessed;
	}
	
	
}