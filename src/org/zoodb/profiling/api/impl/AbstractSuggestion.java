package org.zoodb.profiling.api.impl;

/**
 * @author tobiasg
 *
 */
public abstract class AbstractSuggestion {
	
	private String description;
	
	/**
	 * @returns a text description of this suggestion
	 */
	public String getText() {
		return description;
	}
	
	/**
	 * @param description
	 */
	public void setText(String description) {
		this.description = description;
	}
	
	

}
