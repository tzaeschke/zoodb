package org.zoodb.jdo.internal.query;


/**
 * Query parameter instances are used to assign values to parameters in query after compilation.
 * 
 * @author Tilmann Zäschke
 */
public final class QueryParameter {
	private final String type;
	private final String name;
	private Object value;
	public QueryParameter(String parameter) {
		int i = parameter.indexOf(' ');
		this.type = parameter.substring(0, i);
		this.name = parameter.substring(i+1);
	}
	public void setValue(Object p1) {
		value = p1;
	}
	public Object getValue() {
		return value;
	}
	public Object getName() {
		return name;
	}

}