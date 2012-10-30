package org.zoodb.profiling.api.impl;

import org.zoodb.profiling.api.IPathTree;
import org.zoodb.profiling.api.IPathTreeNode;
import org.zoodb.profiling.api.tree.impl.ClazzNode;

/**
 * @author tobiasg
 *
 */
public class ListSuggestion extends AbstractSuggestion {
	
	private Object list;
	private String description;
	
	public ListSuggestion(Object list) {
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
