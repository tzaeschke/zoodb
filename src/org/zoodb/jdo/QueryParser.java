package org.zoodb.jdo;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOFatalInternalException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.QueryImpl.QueryParameter;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.DatabaseLogger;


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
	private final ZooClassDef _clsDef;
	private final Map<String, Field> _fields;
	@Deprecated
	private Class<?> _minRequiredClass = PersistenceCapableImpl.class;
	
	class QueryTerm {
		private final String _fieldName;
		private final COMP_OP _op;
		private final String _paramName;
		private Object _value;
		private final Class<?> _type;
		private QueryParameter _param;
		private final ZooFieldDef _fieldDef;
		
		public QueryTerm(String fName, COMP_OP op, String paramName,
				Object value, Class<?> type) {
			_fieldName = fName;
			_op = op;
			_paramName = paramName;
			_value = value;
			_type = type;
			_fieldDef = _clsDef.getField(fName);
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
		private QueryTreeNode _n1;
		private QueryTreeNode _n2;
		private QueryTerm _t1;
		private QueryTerm _t2;
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
			isUnary = (_n2==null) && (_t2==null);
		}

		QueryTreeNode relateToChildren() {
			if (_n1 != null) {
				_n1._p = this;
			}
			if (_n2 != null) {
				_n2._p = this;
			}
			return this;
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

		QueryTreeNode root() {
			return _p == null ? this : _p.root();
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

		public QueryTreeNode createSubs(List<QueryTreeNode> subQueriesCandidates) {
			if (LOG_OP.OR.equals(_op)) {
				QueryTreeNode n1;
				QueryTerm t1;
				if (_n1 != null) {
					n1 = _n1.cloneBranch();
					t1 = null;
				} else {
					n1 = null;
					t1 = _t1;
				}
				QueryTreeNode n2;
				QueryTerm t2;
				if (_n2 != null) {
					n2 = _n2.cloneBranch();
					t2 = null;
				} else {
					n2 = null;
					t2 = _t2;
				}
				//we remove the OR from the tree
				QueryTreeNode newTree;
				if (_p != null) {
					//remove local OR and replace with n1/t1
					//clone and replace with child number n2/t2
					if (_p._n1 == this) {
						_p._n1 = n1;
						_p._t1 = t1;
						_p.relateToChildren();
						newTree = cloneSingle(n2, t2, null, null);
					} else if (_p._n2 == this) {
						_p._n2 = n1;
						_p._t2 = t1;
						_p.relateToChildren();
						newTree = cloneSingle(null, null, n2, t2);
					} else {
						//TODO remove
						throw new IllegalStateException();
					}
				}
				//TODO merge with if statements above
				if (_n2 == null) {
					newTree = new QueryTreeNode(null, t2, null, null, null);
				} else {
					newTree = _p.cloneTrunk(this, n2);  //TODO term should also be 0!
				}
				subQueriesCandidates.add(newTree.root());
			}
			
			//go into sub-nodes
			if (_n1 != null) {
				_n1.createSubs(subQueriesCandidates);
			}
			if (_n2 != null) {
				_n2.createSubs(subQueriesCandidates);
			}
			//only required for top level return
			return this;
		}
		
		private QueryTreeNode cloneSingle(QueryTreeNode n1, QueryTerm t1, QueryTreeNode n2,
				QueryTerm t2) {
			QueryTreeNode ret = new QueryTreeNode(n1, t1, _op, n2, t2).relateToChildren();
			return ret;
		}
		
		private QueryTreeNode cloneTrunk(QueryTreeNode stop, QueryTreeNode stopClone) {
			QueryTreeNode n1 = null;
			if (_n1 != null) {
				n1 = (_n1 == stop ? stopClone : _n1.cloneBranch());
			}
			QueryTreeNode n2 = null;
			if (_n2 != null) {
				n2 = _n2 == stop ? stopClone : _n2.cloneBranch();
			}
			
			QueryTreeNode ret = cloneSingle(n1, _t1, n2, _t2);
			if (_p != null) {
				_p.cloneTrunk(this, ret);
			}
			ret.relateToChildren();
			return ret;
		}
		
		private QueryTreeNode cloneBranch() {
			QueryTreeNode n1 = null;
			if (_n1 != null) {
				n1 = _n1.cloneBranch();
			}
			QueryTreeNode n2 = null;
			if (_n2 != null) {
				n2 = _n2.cloneBranch();
			}
			return cloneSingle(n1, _t1, n2, _t2);
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

    QueryParser(String query, Class<?> candidateClass, Map<String, Field> fields, 
    		ZooClassDef clsDef) {
		_str = query; 
		_cls = candidateClass;
		_fields = fields;
		_clsDef = clsDef;
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
	 * 
	 * @param ofs
	 * @return Whether the string is finished after the givven offset
	 */
	private boolean isFinished(int ofs) {
		return !(_pos + ofs < _str.length());
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

		if (isFinished()) {
			return new QueryTreeNode(qn1, qt1, null, null, null).relateToChildren();
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
            return new QueryTreeNode(qn1, qt1, null, null, null).relateToChildren(); //TODO correct?
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
					&& (isFinished(4) || charAt(4)==' ' || charAt(4)==')')) {
				value = true;
				inc(4);
			} else if (substring(pos0, pos0+5).toLowerCase().equals("false") 
					&& (isFinished(5) || charAt(5)==' ' || charAt(5)==')')) {
				value = false;
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

	/**
	 * 
	 * @return min class
	 * @deprecated What is this good for???
	 */
	Class<?> getMinRequiredClass() {
		return _minRequiredClass;
	}

	/**
	 * Deteremine index to use.
	 * 
	 * Policy:
	 * 1) Check if index are available. If not, do not perform any further query analysis (for now)
	 *    -> Query rewriting may still be able to optimize really stupid queries.
	 * 2) Create sub-queries
	 * 3) Analyse sub-queries to determine best index to use. Result may imply that index usage is
	 *    pointless (whole index range required). This could also be if one sub-query does not use
	 *    any index, in which case using an index for the rest slightly increases disk access 
	 *    (index read) but reduces CPU needs (only sub-query to process, not whole query).
	 * 4a) For each sub-query, determine index with smallest range/density.
	 * 4b) Check for required sorting. Using an according index can be of advantage, even if range 
	 *    is larger.
	 * 5) Merge queries with same index and overlapping ranges
	 * 6) merge results
	 * 
	 * @param queryTree
	 * @return Index to use.
	 */
	ZooFieldDef determineIndexToUse(QueryTreeNode queryTree) {
		List<ZooFieldDef> availableIndices = new LinkedList<ZooFieldDef>();
		for (ZooFieldDef f: _clsDef.getAllFields()) {
			if (f.isIndexed()) {
				availableIndices.add(f);
			}
		}
		// step 1
		if (availableIndices.isEmpty()) {
			return null;
		}
		
		//step 2 - sub-queries
		List<QueryTreeNode> subQueries = new LinkedList<QueryParser.QueryTreeNode>();
		List<QueryTreeNode> subQueriesCandidates = new LinkedList<QueryParser.QueryTreeNode>();
		subQueriesCandidates.add(queryTree);
		while (!subQueriesCandidates.isEmpty()) {
			subQueries.add(subQueriesCandidates.remove(0).createSubs(subQueriesCandidates));
		}
		
		//TODO determine this List directly by assigning ZooFields to term during parsing?
		List<ZooFieldDef> usedIndices = new LinkedList<ZooFieldDef>();
		Map<ZooFieldDef, Long> minMap = new IdentityHashMap<ZooFieldDef, Long>();
		Map<ZooFieldDef, Long> maxMap = new IdentityHashMap<ZooFieldDef, Long>();
		QueryTreeIterator iter = queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			ZooFieldDef f = term._fieldDef;
			Long minVal = minMap.get(f);
			if (minVal == null) {
				minMap.put(f, f.getMinValue());
				maxMap.put(f, f.getMaxValue());
			}
//			if (!term.isParametrized()) {
//				continue;
//			}
//			String pName = term.getParamName();
//			//TODO cache Fields in QueryTerm to avoid String comparison?
//			boolean isAssigned = false;
//			for (QueryParameter param: _parameters) {
//				if (pName.equals(param._name)) {
//					//TODO assigning a parameter instead of the value means that the query will
//					//adapt new values even if it is not recompiled.
//					term.setParameter(param);
//					isAssigned = true;
//					break;
//				}
//			}
//			//TODO Exception?
//			if (!isAssigned) {
//				System.out.println("WARNING: Query parameter is not assigned: \"" + pName + "\"");
//			}
		}
		
		if (minMap.isEmpty()) {
			return null;
		}
		
		if (minMap.size() == 1) {
			//TODO provide shortcut by always keeping a ref to the last used index in the loop above?
			return minMap.keySet().iterator().next();
		}
		
		//TODO implement better algorithm!
		ZooFieldDef def = minMap.keySet().iterator().next();
		DatabaseLogger.debugPrintln(0, "Using random index: " + def.getName());
		System.out.println("Using random index: " + def.getName());
		return def;
	}
}
