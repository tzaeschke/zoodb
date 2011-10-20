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
		//TODO split manually i.o. RegEx?
		String[] res = parameter.split(" ");
		this.type = res[0];
		this.name = res[1];
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