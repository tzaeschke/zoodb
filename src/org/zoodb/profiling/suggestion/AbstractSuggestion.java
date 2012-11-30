package org.zoodb.profiling.suggestion;

/**
 * @author tobiasg
 *
 */
public abstract class AbstractSuggestion {
	
	private String identifier;
	
	/**
	 * Class to which this suggestion belongs to
	 */
	private String clazzName;
	
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
		return clazzName;
	}

	public void setClazzName(String clazzName) {
		this.clazzName = clazzName;
	}
	
		
	/**
	 * Applies suggestion to model object
	 * @param model
	 */
	public abstract void apply(Object model);
	
	
	/**
	 * Provide a text description of this suggestion in column 'columnIndex'
	 * This will be used in the view, so each suggestion can decide itself hot it should be displayed
	 * @param columnIndex
	 * @return
	 */
	public String provideLabelForColumn(int columnIndex) {
			switch(columnIndex) {
				case 0:
					return getText();
				case 1:
					return getClazzName();
				case 2:
					return getIdentifier();
				default:
					return null;
			}
	}

	public String getIdentifier() {
		return identifier;
	}
	
}