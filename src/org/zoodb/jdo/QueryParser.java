package org.zoodb.jdo;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOFatalInternalException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.QueryImpl.QueryParameter;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class QueryParser {

	private static final Object NULL = new Object();
	
	private int _pos = 0;
	private final String _str;
	private final Class<?> _cls;
	private final Map<String, Field> _fields;
	private Class<?> _minRequiredClass = PersistenceCapableImpl.class;
	
	class QueryTerm {

		private final String _fieldName;
		private final COMP_OP _op;
		private final String _paramName;
		private Object _value;
		private final Class<?> _type;
		private QueryParameter _param;
		
		public QueryTerm(String fName, COMP_OP op, String paramName,
				Object value, Class<?> type) {
			_fieldName = fName;
			_op = op;
			_paramName = paramName;
			_value = value;
			_type = type;
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
			Field f = _fields.get(_fieldName);

			Object oVal;
			try {
				oVal = f.get(o);
			} catch (IllegalArgumentException e) {
				throw new JDOFatalInternalException("Can not access field: " + _fieldName + " cl=" +
						o.getClass().getName() + " fcl=" + f.getDeclaringClass().getName(), e);
			} catch (IllegalAccessException e) {
				throw new JDOFatalInternalException("Can not access field: " + _fieldName, e);
			}
			//TODO avoid indirection and store Parameter value in local _value field !!!!!!!!!!!!!!!!
			Object qVal = getValue();
			if (oVal == null && qVal == NULL) {
				return true;
			} else if (qVal != NULL) {
				if (qVal.equals(oVal) && (_op==COMP_OP.EQ || _op==COMP_OP.LE || _op==COMP_OP.ME)) {
					return true;
				}
				if (qVal instanceof Comparable) {
					Comparable qComp = (Comparable) qVal;
					int res = qComp.compareTo(oVal);  //-1:<   0:==  1:> 
					if (res == 1 && (_op == COMP_OP.LE || _op==COMP_OP.L || _op==COMP_OP.NE)) {
						return true;
					} else if (res == -1 && (_op == COMP_OP.ME || _op==COMP_OP.M || _op==COMP_OP.NE)) {
						return true;
					}
				}
			}
			return false;
		}
		
	}

	
	static class QueryTreeNode {
		private final QueryTreeNode _n1;
		private final QueryTreeNode _n2;
		private final QueryTerm _t1;
		private final QueryTerm _t2;
		private final LOG_OP _op;
		private QueryTreeNode _p;

		QueryTreeNode(QueryTreeNode n1, QueryTerm t1, LOG_OP op, QueryTreeNode n2, QueryTerm t2) {
			_n1 = n1;
			_t1 = t1;
			_n2 = n2;
			_t2 = t2;
			_op = op;
			if (n1 != null) {
				n1._p = this;
			}
			if (n2 != null) {
				n2._p = this;
			}
		}

		QueryTerm firstTerm() {
			return _t1;
		}

		QueryTreeNode firstNode() {
			return _n1;
		}

		QueryTerm secondTerm() {
			return _t2;
		}

		QueryTreeNode secondNode() {
			return _n2;
		}

		QueryTreeNode parent() {
			return _p;
		}

		public QueryTreeIterator termIterator() {
			if (_p != null) {
				throw new JDOFatalDataStoreException("Can not get iterator of child elements.");
			}
			return new QueryTreeIterator(this);
		}

		public boolean evaluate(Object o) {
			boolean first = (_n1 != null ? _n1.evaluate(o) : _t1.evaluate(o));
			//do we have a second part?
			if (_op == null) {
				return first;
			}
			if ( !first && _op == LOG_OP.AND) {
				return false;
			}
			if ( first && _op == LOG_OP.OR) {
				return true;
			}
			return (_n2 != null ? _n2.evaluate(o) : _t2.evaluate(o));
		}
	}

	static class QueryTreeIterator {
		private QueryTreeNode _currentNode;
		private boolean askFirst = true;

		private QueryTreeIterator(QueryTreeNode node) {
			_currentNode = node;
		}

		boolean hasNext() {
			if (_currentNode == null) {
				return false;  //TODO can that happen?
			}
			if (_currentNode.parent() != null) {
				return true;
			}
			if (askFirst) {
				return true;
			}
			
			//status: has no parent, pointing to second node
			return false;
		}
		
		QueryTerm next() {
			if (askFirst) {
				while (_currentNode.firstTerm() == null) {
					_currentNode = _currentNode.firstNode(); 
				}
				askFirst = false;
				return _currentNode.firstTerm();
			} else {
				if (_currentNode.secondTerm() != null) {
					_currentNode = _currentNode.parent();
					return _currentNode.secondTerm();
				} else {
					_currentNode = _currentNode.secondNode();
					if (_currentNode == null) {
						throw new NoSuchElementException();
					}
					askFirst = true;
					return next();
				}
			}
		}
	}
	
	public QueryParser(String query, Class<?> candidateClass, Map<String, Field> fields) {
		_str = query; 
		_cls = candidateClass;
		_fields = fields;
	}
	
	private void trim() {
		while (!isFinished() && charAt0() == ' ') {
			_pos ++;
		}
	}
	
	private char charAt0() {
		return _str.charAt(_pos);
	}
	
	private char charAt(int i) {
		return _str.charAt(_pos + i);
	}
	
	private void inc() {
		_pos++;
	}
	
	private void inc(int i) {
		_pos += i;
	}
	
	private int pos() {
		return _pos;
	}
	
	private boolean isFinished() {
		return !(_pos < _str.length());
	}
	
	/**
	 * @return remaining length.
	 */
	private int len() {
		return _str.length() - _pos;
	}

	
	/**
	 * 
	 * @param pos0 start, absolute position, inclusive
	 * @param pos1 end, absolute position, exclusive
	 * @return sub-String
	 */
	private String substring(int pos0, int pos1) {
		return _str.substring(pos0, pos1);
	}
	
	QueryTreeNode parseQuery() {
		QueryTreeNode qn = parseTree();
		while (!isFinished()) {
			qn = parseTree(null, qn);
		}
		return qn;
	}
	
	private QueryTreeNode parseTree() {
		trim();
		QueryTerm qt1 = null;
		QueryTreeNode qn1 = null;
		if (charAt0() == '(') {
			inc();
			qn1 = parseTree();
			trim();
			if (charAt0() != ')') {
				throw new JDOUserException("Missing closing bracket at position " + pos() + ": ",
						_str);
			}
		} else {
			qt1 = parseTerm();
		}

		if(isFinished()) {
			return new QueryTreeNode(qn1, qt1, null, null, null);
		}
		
		//parse log op
		char c = charAt0();
		char c2 = charAt(1);
		char c3 = charAt(2);
		LOG_OP op = null;
		if (c == '&' && c2 ==  '&' && c3 == ' ') {
			op = LOG_OP.AND;
		} else if (c == '|' && c2 ==  '|' && c3 == ' ') {
			
		} else {
			throw new JDOUserException(
					"Unexpected characters: '" + c + c2 + c3 + "' at: " + pos());
		}
		inc( op._len );
		trim();

		
		// read next term
		QueryTerm qt2 = null;
		QueryTreeNode qn2 = null;
		if (charAt0() == '(') {
			inc();
			qn2 = parseTree();
			trim();
			if (charAt0() != ')') {
				throw new JDOUserException("Missing closing bracket at position " + pos() + ": ",
						_str);
			}
		} else {
			qt2 = parseTerm();
		}

		return new QueryTreeNode(qn1, qt1, op, qn2, qt2);
	}
	
	private QueryTreeNode parseTree(QueryTerm qt1, QueryTreeNode qn1) {
		trim();

		//parse log op
		char c = charAt0();
		char c2 = charAt(1);
		char c3 = charAt(2);
		LOG_OP op = null;
		if (c == '&' && c2 ==  '&' && c3 == ' ') {
			op = LOG_OP.AND;
		} else if (c == '|' && c2 ==  '|' && c3 == ' ') {
			
		} else {
			throw new JDOUserException(
					"Unexpected characters: '" + c + c2 + c3 + "' at: " + pos());
		}
		inc( op._len );
		trim();

		
		// read next term
		QueryTerm qt2 = null;
		QueryTreeNode qn2 = null;
		if (charAt0() == '(') {
			inc();
			qn2 = parseTree();
			trim();
			if (charAt0() != ')') {
				throw new JDOUserException("Missing closing bracket at position " + pos() + ": ",
						_str);
			}
		} else {
			qt2 = parseTerm();
		}

		return new QueryTreeNode(qn1, qt1, op, qn2, qt2);
	}

	private QueryTerm parseTerm() {
		trim();
		if (charAt0() == '(') {
			//TODO remove, should never happen
			throw new UnsupportedOperationException();
		}
		Object value = null;
		String paramName = null;
		COMP_OP op = null;
		String fName = null;
		Class<?> type = null;

		int pos0 = pos();

		//read field name
		char c = charAt0();
		while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') 
				|| (c=='_') || (c=='.')) {
			inc();
			if (c=='.') {
				String dummy = substring(pos0, pos());
				if (dummy.equals("this.")) {
					//System.out.println("STUB QueryParser.parseTerm(): Ignoring 'this.'.");
					pos0 = pos();
				} else {
					fName = substring(pos0, pos());
					pos0 = pos();
					//TODO
//					if (startsWith("")) {
//
//					} else if (startsWith("")) {
//						
//					} else {
//						throw new JDOUserException("Can not parse query at position " + pos0 + 
//								": " + dummy);
//					}
				}
			}
			c = charAt0();
		}
		if (fName == null) {
			fName = substring(pos0, pos());
		}
		if (fName.equals("")) {
			throw new JDOUserException("Can not parse query at position " + pos0 + ".");
		}
		pos0 = pos();
		trim();

		try {
			Field f = _fields.get(fName);
			if (f == null) {
				throw new JDOFatalInternalException("Field name not found: " + fName);
			}
			type = f.getType();
			//This is required to later derive the required classes (we could do this here...).
			//These are required to avoid running the query on super classes that do not have
			//certain attributes.
			if (!f.getDeclaringClass().isAssignableFrom(_minRequiredClass)) {
				_minRequiredClass = f.getDeclaringClass();
			}
		} catch (SecurityException e) {
			throw new JDOUserException("Field not accessible: " + fName, e);
		}


		//read operator
		c = charAt0();
		char c2 = charAt(1);
		char c3 = charAt(2);
		if (c == '=' && c2 ==  '=' && c3 == ' ') {
			op = COMP_OP.EQ;
		} else if (c == '<') {
			if (c2 ==  '=' && c3 == ' ') {
				op = COMP_OP.LE;
			} else if (c2 == ' ') {
				op = COMP_OP.L;
			}
		} else if (c == '>') {
			if (c2 ==  '=' && c3 == ' ') {
				op = COMP_OP.ME;
			} else if (c2 == ' ') {
				op = COMP_OP.M;
			}
		} else if (c == '!' && c2 == '=' && c3 == ' ') {
			op = COMP_OP.NE;
		}
		if (op == null) {
			throw new JDOUserException(
					"Unexpected characters: '" + c + c2 + c3 + "' at: " + pos0);
		}
		inc( op._len );
		trim();
		pos0 = pos();

		//read value
		c = charAt0();
		if ((len() >= 4 && substring(pos0, pos0+4).equals("null")) &&
				(len() == 4 || (len()>4 && (charAt(4) == ' ' || charAt(4) == ')')))) {  //hehehe :-)
			if (type.isPrimitive()) {
				throw new JDOUserException("Cannot compare 'null' to primitive at pos:" + pos0);
			}
			value = NULL;
		} else if (c=='"' || c=='\'') {
			//According to JDO 2.2 14.6.2, String and single characters can both be delimited by 
			//both single and double quotes.
			boolean singleQuote = c == '\''; 
			//TODO allow char type!
			if (!String.class.isAssignableFrom(type)) {
				throw new JDOUserException("Incompatible types, found String, expected: " + 
						type.getName());
			}
			inc();
			pos0 = pos();
			c = charAt0();
			while (true) {
				if ( (!singleQuote && c=='"') || (singleQuote && c=='\'')) {
					break;
				} else if (c=='\\') {
					inc();
				}					
				inc();
				c = charAt0();
			}
			value = substring(pos0, pos());
		} else if (c=='-' || (c > '0' && c < '9')) {
			pos0 = pos();
			boolean isHex = false;
			while (!isFinished()) {
				c = charAt0();
				if (c==')' || c==' ') {
					break;
//				} else if (c=='.') {
//					isDouble = true;
//				} else if (c=='L' || c=='l') {
//					//if this is not at the last position, then we will fail later anyway
//					isLong = true;
				} else if (c=='x') {
					//if this is not at the last position, then we will fail later anyway
					isHex = true;
				}
				inc();
			}
			if (type == Double.TYPE || type == Double.class) {
				value = Double.parseDouble( substring(pos0, pos()) );
			} else if (type == Float.TYPE || type == Float.class) {
				value = Float.parseFloat( substring(pos0, pos()) );
			} else if (type == Long.TYPE || type == Long.class) {
				if (isHex) {
					value = Long.parseLong( substring(pos0, pos()), 16 );
				} else {
					value = Long.parseLong( substring(pos0, pos()));
				}
			} else if (type == Integer.TYPE || type == Integer.class) {
				if (isHex) {
					value = Integer.parseInt( substring(pos0, pos()), 16 );
				} else {
					value = Integer.parseInt( substring(pos0, pos()) );
				}
			} else if (type == Short.TYPE || type == Short.class) {
				if (isHex) {
					value = Short.parseShort( substring(pos0, pos()), 16 );
				} else {
					value = Short.parseShort( substring(pos0, pos()) );
				}
			} else if (type == Byte.TYPE || type == Byte.class) {
				if (isHex) {
					value = Byte.parseByte( substring(pos0, pos()), 16 );
				} else {
					value = Byte.parseByte( substring(pos0, pos()) );
				}
			} else if (type == BigDecimal.class) {
				value = new BigDecimal( substring(pos0, pos()) );
			} else if (type == BigInteger.class) {
				value = new BigInteger( substring(pos0, pos()) );
			} else { 
				throw new JDOUserException("Incompatible types, found number, expected: " + 
						type.getName());
			}
		} else if (type == Boolean.TYPE || type == Boolean.class) {
			if (substring(pos0, pos0+4).toLowerCase().equals("true") 
					&& (charAt(4)==' ' || charAt(4)==')')) {
				value = true;
				inc(4);
			} else if (substring(pos0, pos0+5).toLowerCase().equals("false") 
					&& (charAt(5)==' ' || charAt(5)==')')) {
				value = true;
				inc(5);
			} else {
			throw new JDOUserException("Incompatible types, expected Boolean, found: " + 
					substring(pos0, pos0+5));
			}
		} else {
			while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c=='_') || (c=='"')) {
				inc();
				if (isFinished()) break;
				c = charAt0();
			}
			paramName = substring(pos0, pos());
		}
		if (fName == null || (value == null && paramName == null) || op == null) {
			throw new JDOUserException("Can not parse query at " + pos() + ": " + _str);
		}
		trim();
		
		return new QueryTerm(fName, op, paramName, value, type);
	}

	private enum COMP_OP {
		EQ(2), NE(2), LE(2), ME(2), L(1), M(1);

		private final int _len;

		private COMP_OP(int len) {
			_len = len;
		}
	}

	/**
	 * Logical operators.
	 */
	private enum LOG_OP {
		AND(2), // && 
		OR(2);  // ||
		//XOR(2);  
		// NOT(?);

		private final int _len;

		private LOG_OP(int len) {
			_len = len;
		}
	}

	Class<?> getMinRequiredClass() {
		return _minRequiredClass;
	}
}
