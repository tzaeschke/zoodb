package org.zoodb.profiling.api.impl;

import org.zoodb.profiling.api.IPathTree;
import org.zoodb.profiling.api.IPathTreeNode;

/**
 * @author tobiasg
 *
 */
public class ListSuggestion extends AbstractSuggestion {
	
	private IPathTree list;
	private String description;
	
	public ListSuggestion(IPathTree list) {
		this.list = list;
	}

	@Override
	public String getText() {
		return description;
	}
	
	public void setText(String description) {
		this.description = description;
	}
	
	
	
	

}
