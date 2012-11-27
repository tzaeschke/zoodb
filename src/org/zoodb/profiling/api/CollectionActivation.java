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
	
	//safe to remove if forward traversal is implemented (children are available)
	@Deprecated
	private boolean accessed;

	/**
	 * Generic type of this collection 
	 */
	private Class<?> type;
	
	
	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	@Deprecated
	public boolean isAccessed() {
		return accessed;
	}
	
	@Deprecated
	public void setAccessed(boolean accessed) {
		this.accessed = accessed;
	}

	public Class<?> getType() {
		return type;
	}

	public void setType(Class<?> type) {
		this.type = type;
	}
	
	
	
	
}