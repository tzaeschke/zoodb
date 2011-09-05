package org.zoodb.jdo.internal.query;

import java.lang.reflect.Field;

import javax.jdo.JDOFatalInternalException;

import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.query.QueryParser.COMP_OP;

public final class QueryTerm {

	private final COMP_OP _op;
	private final String _paramName;
	private Object _value;
	private QueryParameter _param;
	private final ZooFieldDef _fieldDef;
	
	public QueryTerm(COMP_OP op, String paramName,
			Object value, ZooFieldDef fieldDef) {
		_op = op;
		_paramName = paramName;
		_value = value;
		_fieldDef = fieldDef;
	}

	public boolean isParametrized() {
		return _paramName != null;
	}

	public String getParamName() {
		return _paramName;
	}

	public void setParameter(QueryParameter param) {
		_param = param;
	}
	
	public QueryParameter getParameter() {
		return _param;
	}
	
	public Object getValue() {
		if (_paramName != null) {
			return _param.getValue();
		}
		return _value;
	}

	public boolean evaluate(Object o) {
		// we can not cache this, because sub-classes may have different field instances.
		//TODO cache per class? Or reset after query has processed first class set?
		Field f = _fieldDef.getJavaField();

		Object oVal;
		try {
			oVal = f.get(o);
		} catch (IllegalArgumentException e) {
			throw new JDOFatalInternalException("Can not access field: " + _fieldDef.getName() + 
					" cl=" + o.getClass().getName() + " fcl=" + f.getDeclaringClass().getName(), e);
		} catch (IllegalAccessException e) {
			throw new JDOFatalInternalException("Can not access field: " + _fieldDef.getName(), e);
		}
		//TODO avoid indirection and store Parameter value in local _value field !!!!!!!!!!!!!!!!
		Object qVal = getValue();
		if (oVal == null && qVal == QueryParser.NULL) {
			return true;
		} else if (qVal != QueryParser.NULL) {
			if (qVal.equals(oVal) && (_op==COMP_OP.EQ || _op==COMP_OP.LE || _op==COMP_OP.AE)) {
				return true;
			}
			if (qVal instanceof Comparable) {
				Comparable qComp = (Comparable) qVal;
				int res = qComp.compareTo(oVal);  //-1:<   0:==  1:> 
				if (res == 1 && (_op == COMP_OP.LE || _op==COMP_OP.L || _op==COMP_OP.NE)) {
					return true;
				} else if (res == -1 && (_op == COMP_OP.AE || _op==COMP_OP.A || _op==COMP_OP.NE)) {
					return true;
				}
			}
		}
		return false;
	}

	public String print() {
		StringBuilder sb = new StringBuilder();
		sb.append(_fieldDef.getName());
		sb.append(" ");
		sb.append(_op);
		sb.append(" ");
		sb.append(_value);
		return sb.toString();
	}

	ZooFieldDef getFieldDef() {
		return _fieldDef;
	}

	COMP_OP getOp() {
		return _op;
	}
	
}