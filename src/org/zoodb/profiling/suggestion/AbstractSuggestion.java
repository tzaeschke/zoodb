package org.zoodb.profiling.suggestion;

/**
 * @author tobiasg
 *
 */
public abstract class AbstractSuggestion {
	
	/**
	 * Class to which this suggestion belongs to
	 */
	private Class<?> clazz;
	
	/**
	 * text description of this suggestion
	 */
	private String description;
	

	
	public String getText() {
		return description;
	}
	
	public void setText(String description) {
		this.description = description;
	}

	public String getClazzName() {
		return clazz.getName();
	}

	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}
	
	public Class<?> getClazz() {
		return clazz;
	}
	
	/**
	 * Applies suggestion to model object
	 * @param model
	 */
	public abstract void apply(Object model);
	
}