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


/**
 * The query parser. This class builds a query tree from a query string.
 * The tree consists of QueryTerms (comparative statements) and QueryNodes (logical operations on
 * two children (QueryTerms or QueryNodes).
 * The root of the tree is a QueryNode. QueryNodes may have only a single child.  
 * 
 * 
 * TODO QueryOptimiser:
 * E.g. "((( A==B )))"Will create something like Node->Node->Node->Term. Optimise this to 
 * Node->Term. That means pulling up all terms where the parent node has no other children. The
 * only exception is the root node, which is allowed to have only one child.
 * 
 * @author Tilmann Zäschke
 */
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
		/** tell whether there is more than one child attached.
		    root nodes and !() node have only one child. */
		private final boolean isUnary; 
		

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
			isUnary = (_n2==null) && (_t2==null);
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

	
    /**
     * QueryIterator class.
     */
    static class QueryTreeIterator {
        private QueryTreeNode _currentNode;
        //private boolean askFirst = true;
        private final boolean[] askFirstA = new boolean[20]; //0==first; 1==2nd; 2==done
        private int askFirstD = 0; //depth: 0=root
        private QueryTerm nextElement = null;

        private QueryTreeIterator(QueryTreeNode node) {
            _currentNode = node;
            for (int i = 0; i < askFirstA.length; i++) {
                askFirstA[i] = true;
            }
            nextElement = findNext();
        }

        
        boolean hasNext() {
            return (nextElement != null);
        }
        

        /**
         * To avoid duplicate work in next() and hasNext() (both can locate the next element),
         * we automatically move to the next element when the previous is returned. The hasNext()
         * method then becomes trivial.
         * @return next element.
         */
        QueryTerm next() {
            if (nextElement == null) {
                throw new NoSuchElementException();
            }
            QueryTerm t = nextElement;
            nextElement = findNext();
            return t;
        }
        
        /**
         * Also, ASK nur setzen wenn ich hoch komme?
         * runter: true-> first;  false second;
         * hoch: true-> nextSecond; false-> nextUP
         */
        private QueryTerm findNext() {
            //Walk down first branch
            if (askFirstA[askFirstD]) {
                while (_currentNode.firstTerm() == null) {
                    //remember that we already walked down the first branch
                    askFirstD++;
                    _currentNode = _currentNode.firstNode();
                }
                askFirstA[askFirstD] = false;
                return _currentNode.firstTerm();
            } 
            
            //do we have a second branch?
            if (_currentNode.isUnary) {
                return findUpwards();
            }
                
            //walk down second branch
            if (_currentNode.secondTerm() != null) {
                //dirty hack
                if (_currentNode.secondTerm() != nextElement) {
                    return _currentNode.secondTerm();
                }
                //else: we have been here before!
                //walk back up
                return findUpwards();
            } else {
                _currentNode = _currentNode.secondNode();
                askFirstD++;
                return findNext();
            }
        }

        private QueryTerm findUpwards() {
            if (_currentNode._p == null) {
                return null;
            }
            
            do {
                //clean up behind me before moving back up
                askFirstA[askFirstD] = true;
                askFirstD--;
                _currentNode = _currentNode.parent();
            } while (_currentNode._p != null && (_currentNode.isUnary || !askFirstA[askFirstD]));

            //remove, only for DEBUG
//            if (_currentNode == null) {
//                throw new NoSuchElementException();
//            }
            //if 'false' then we are finished 
            if (!askFirstA[askFirstD]) {
                return null;
            }
            //indicate that we want the second branch now
            askFirstA[askFirstD] = false;
            //walk down second branch
            return findNext();
        }
    }

    QueryParser(String query, Class<?> candidateClass, Map<String, Field> fields) {
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
		} else {
			qt1 = parseTerm();
		}

		if(isFinished()) {
			return new QueryTreeNode(qn1, qt1, null, null, null);
		}
		
		//parse log op
		char c = charAt0();
        if (c == ')') {
            inc( 1 );
            trim();
            if (qt1 == null) {
                return qn1;
            } else {
                return new QueryTreeNode(qn1, qt1, null, null, null);
            }
            //throw new UnsupportedOperationException();
        }
		char c2 = charAt(1);
		char c3 = charAt(2);
		LOG_OP op = null;
		if (c == '&' && c2 ==  '&' && c3 == ' ') {
			op = LOG_OP.AND;
		} else if (c == '|' && c2 ==  '|' && c3 == ' ') {
            op = LOG_OP.OR;
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
		} else {
			qt2 = parseTerm();
		}

		return new QueryTreeNode(qn1, qt1, op, qn2, qt2);
	}
	
	private QueryTreeNode parseTree(QueryTerm qt1, QueryTreeNode qn1) {
		trim();

		//parse log op
		char c = charAt0();
        if (c == ')') {
            inc(1);
            trim();
            return new QueryTreeNode(qn1, qt1, null, null, null); //TODO correct?
        }
		char c2 = charAt(1);
		char c3 = charAt(2);
		LOG_OP op = null;
        if (c == '&' && c2 ==  '&' && c3 == ' ') {
			op = LOG_OP.AND;
		} else if (c == '|' && c2 ==  '|' && c3 == ' ') {
            op = LOG_OP.OR;
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
					fName = substring(pos0, pos()-1);
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
			inc();
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
		EQ(2, false, false, true), 
		NE(2, true, true, false), 
		LE(2, true, false, true), 
		ME(2, false, true, true), 
		L(1, true, false, false), 
		M(1, false, true, false);

		private final int _len;
        private final boolean _allowsLess;
        private final boolean _allowsMore;
        private final boolean _allowsEqual;

		private COMP_OP(int len, boolean al, boolean am, boolean ae) {
			_len = len;
            _allowsLess = al; 
            _allowsMore = am; 
            _allowsEqual = ae; 
		}
        
		//TODO use in lines 90-110. Also use as first term(?).
        private boolean allowsLess() {
            return _allowsLess;
        }
        
        private boolean allowsMore() {
            return _allowsMore;
        }
        
        private boolean allowsEqual() {
            return _allowsEqual;
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
