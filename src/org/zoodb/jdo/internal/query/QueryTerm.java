package org.zoodb.jdo.internal.query;

import java.lang.reflect.Field;

import javax.jdo.JDOFatalInternalException;

import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.query.QueryParser.COMP_OP;

public final class QueryTerm {

	private final COMP_OP op;
	private final String paramName;
	private Object value;
	private QueryParameter _param;
	private final ZooFieldDef fieldDef;
	
	public QueryTerm(COMP_OP op, String paramName,
			Object value, ZooFieldDef fieldDef) {
		this.op = op;
		this.paramName = paramName;
		this.value = value;
		this.fieldDef = fieldDef;
	}

	public boolean isParametrized() {
		return paramName != null;
	}

	public String getParamName() {
		return paramName;
	}

	public void setParameter(QueryParameter param) {
		_param = param;
	}
	
	public QueryParameter getParameter() {
		return _param;
	}
	
	public Object getValue() {
		if (paramName != null) {
			return _param.getValue();
		}
		return value;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean evaluate(Object o) {
		// we can not cache this, because sub-classes may have different field instances.
		//TODO cache per class? Or reset after query has processed first class set?
		Field f = fieldDef.getJavaField();

		Object oVal;
		try {
			oVal = f.get(o);
		} catch (IllegalArgumentException e) {
			throw new JDOFatalInternalException("Can not access field: " + fieldDef.getName() + 
					" cl=" + o.getClass().getName() + " fcl=" + f.getDeclaringClass().getName(), e);
		} catch (IllegalAccessException e) {
			throw new JDOFatalInternalException("Can not access field: " + fieldDef.getName(), e);
		}
		//TODO avoid indirection and store Parameter value in local _value field !!!!!!!!!!!!!!!!
		Object qVal = getValue();
		if (oVal == null && qVal == QueryParser.NULL) {
			return true;
		} else if (qVal != QueryParser.NULL) {
			if (qVal.equals(oVal) && (op==COMP_OP.EQ || op==COMP_OP.LE || op==COMP_OP.AE)) {
				return true;
			}
			if (qVal instanceof Comparable) {
				Comparable qComp = (Comparable) qVal;
				int res = qComp.compareTo(oVal);  //-1:<   0:==  1:> 
				if (res == 1 && (op == COMP_OP.LE || op==COMP_OP.L || op==COMP_OP.NE)) {
					return true;
				} else if (res == -1 && (op == COMP_OP.AE || op==COMP_OP.A || op==COMP_OP.NE)) {
					return true;
				}
			}
		}
		return false;
	}

	public String print() {
		StringBuilder sb = new StringBuilder();
		sb.append(fieldDef.getName());
		sb.append(" ");
		sb.append(op);
		sb.append(" ");
		sb.append(value);
		return sb.toString();
	}

	ZooFieldDef getFieldDef() {
		return fieldDef;
	}

	COMP_OP getOp() {
		return op;
	}
	
}