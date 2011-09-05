package org.zoodb.jdo.internal.query;


/**
 * Query parameter instances are used to assign values to parameters in query after compilation.
 * 
 * @author Tilmann Zäschke
 */
public final class QueryParameter {
	private final String _type;
	private final String _name;
	private Object _value;
	public QueryParameter(String parameter) {
		//TODO split manually i.o. RegEx?
		String[] res = parameter.split(" ");
		_type = res[0];
		_name = res[1];
	}
	public void setValue(Object p1) {
		_value = p1;
	}
	public Object getValue() {
		return _value;
	}
	public Object getName() {
		return _name;
	}

}