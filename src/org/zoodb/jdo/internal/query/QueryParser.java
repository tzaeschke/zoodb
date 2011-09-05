package org.zoodb.jdo.internal.query;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOFatalInternalException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.server.index.BitTools;
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
public final class QueryParser {

	static final Object NULL = new Object();
	
	private int _pos = 0;
	private final String _str;
	private final ZooClassDef _clsDef;
	private final Map<String, Field> _fields;
	
	public QueryParser(String query, Map<String, Field> fields, ZooClassDef clsDef) {
		_str = query; 
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
	
	public QueryTreeNode parseQuery() {
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
		
		return new QueryTerm(op, paramName, value, _clsDef.getField(fName));
	}

	enum COMP_OP {
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
	enum LOG_OP {
		AND(2), // && 
		OR(2);  // ||
		//XOR(2);  
		// NOT(?);  //TODO e.g. not supported in unary-stripper or in index-advisor

		private final int _len;

		private LOG_OP(int len) {
			_len = len;
		}
	}

	/**
	 * Determine index to use.
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
	public List<QueryAdvice> determineIndexToUse(QueryTreeNode queryTree) {
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
		//We split the query tree at every OR into sub queries, such that every sub-query contains
		//the full query but only one side of every OR. All ORs are removed.
		//-> Optimization: We remove only (and split only at) ORs where at least on branch
		//   uses an index. TODO
		List<QueryTreeNode> subQueries = new LinkedList<QueryTreeNode>();
		List<QueryTreeNode> subQueriesCandidates = new LinkedList<QueryTreeNode>();
		subQueriesCandidates.add(queryTree);
		System.out.println("Query1: " + queryTree.print());
		while (!subQueriesCandidates.isEmpty()) {
			subQueries.add(subQueriesCandidates.remove(0).createSubs(subQueriesCandidates));
		}
		
		System.out.println("Query2: " + queryTree.print());
		for (QueryTreeNode sq: subQueries) {
			optimize(sq);
			System.out.println("Sub-query: " + sq.print());
		}
		//TODO filter out terms that can not become true.
		//if none is left, return empty set.
		
		List<QueryAdvice> advices = new LinkedList<QueryAdvice>();
		for (QueryTreeNode sq: subQueries) {
			advices.add(determineIndexToUseSub(sq));
		}
		
		//TODO merge queries
		//E.g.:
		// - if none uses an index (or at least one doesn't), return only the full query
		// - if ranges overlap, try to merge?
		
		//check for show-stoppers
		//-> in their case, we simply run the un-split query on the full type extent.
		for (QueryAdvice qa: advices) {
			//assuming that the term is not an empty term (contradicting sub-terms)
			if (qa == null) {
				//ah, one of them iterates over the whole result set.
				advices.clear();
				advices.add(qa);
				return advices;
			}
			//TODO instead of fixed values, use min/max of index.
			if (qa.getMin() <= Long.MIN_VALUE && qa.getMax() >= Long.MAX_VALUE) {
				//ah, one of them iterates over the whole result set.
				advices.clear();
				advices.add(qa);
				return advices;
			}
		}
		
		//check for overlapping / global min/max
		long globalMin = Long.MAX_VALUE;
		long globalMax = Long.MIN_VALUE;
		//if they overlap, we should merge them to void duplicate loading effort and results.
		//if they don't overlap, we don't have to care about either.
		//-> assuming they all use the same index... TODO?
		
		
		
		return advices;
	}
	
		
	/**
	 * 
	 * @param queryTree This is a sub-query that does not contain OR operands.
	 * @return QueryAdvise
	 */
	private QueryAdvice determineIndexToUseSub(QueryTreeNode queryTree) {
		//TODO determine this List directly by assigning ZooFields to term during parsing?
		Map<ZooFieldDef, Long> minMap = new IdentityHashMap<ZooFieldDef, Long>();
		Map<ZooFieldDef, Long> maxMap = new IdentityHashMap<ZooFieldDef, Long>();
		QueryTreeIterator iter = queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			ZooFieldDef f = term.getFieldDef();
			if (!f.isIndexed()) {
				//ignore fields that are not index
				continue;
			}
			
			Long minVal = minMap.get(f);
			if (minVal == null) {
				//needs initialization
				minMap.put(f, f.getMinValue());
				maxMap.put(f, f.getMaxValue());
			}
			
			Long value;
			if (term.getValue() instanceof Number) {
				value = ((Number)term.getValue()).longValue();
			} else if (term.getValue() instanceof String) {
				value = BitTools.toSortableLong((String) term.getValue());
			} else if (term.getValue() instanceof Boolean) {
				//pointless..., well pretty much, unless someone uses this to distinguish
				//very few 'true' from many 'false'or vice versa.
				continue;
			} else {
				throw new IllegalArgumentException("Type: " + term.getValue().getClass());
			}
			
			switch (term.getOp()) {
			case EQ: {
				//TODO check range and exit if EQ does not fit in remaining range
				minMap.put(f, value);
				maxMap.put(f, value);
				break;
			}
			case L:
				if (value < maxMap.get(f)) {
					maxMap.put(f, value - 1); //TODO does this work with floats?
				}
				break;
			case LE: 				
				if (value < maxMap.get(f)) {
					maxMap.put(f, value);
				}
				break;
			case M: 
				if (value > minMap.get(f)) {
					minMap.put(f, value + 1); //TODO does this work with floats?
				}
				break;
			case ME:
				if (value > minMap.get(f)) {
					minMap.put(f, value);
				}
				break;
			case NE:
				//ignore
				break;
			default: 
				throw new IllegalArgumentException("Name: " + term.getOp());
			}
			
			//TODO take into accoutn not-operators (x>1 && x<10) && !(x>5 && X <6) ??
			// -> Hopefully this optimization is marginal and negligible.
			//But it may break everything!
		}
		
		if (minMap.isEmpty()) {
			return null;
		}
		
		//the adviced index to use...
		// start with first
		ZooFieldDef def = minMap.keySet().iterator().next();
		QueryAdvice qa = new QueryAdvice(queryTree);
		qa.setIndex( def );
		qa.setMin( minMap.get(def) );
		qa.setMax( maxMap.get(def) );
		
		//only one index left? -> Easy!!!
		//TODO well, better not use it if it covers the whole range? Maybe for sorting?
		if (minMap.size() == 1) {
			qa.setIndex( minMap.keySet().iterator().next() );
			return qa;
		}
		
		for (ZooFieldDef d2: minMap.keySet()) {
			long min2 = minMap.get(d2);
			long max2 = maxMap.get(d2);
			//TODO fix for very large values
			if ((max2-min2) < (qa.getMax() - qa.getMin())) {
				qa.setIndex( d2 );
				qa.setMin( min2 );
				qa.setMax( max2 );
			}
		}
		//TODO implement better algorithm!
//		DatabaseLogger.debugPrintln(0, "Using random index: " + def.getName());
		DatabaseLogger.debugPrintln(0, "Using index: " + def.getName());
//		System.out.println("Using random index: " + def.getName());
		return qa;
	}

	private void optimize(QueryTreeNode q) {
		stripUnaryNodes(q);
	}

	private void stripUnaryNodes(QueryTreeNode q) {
		while (q.isUnary() && q._n1 != null) {
			//this is a unary root node that shouldn't be one
			q._op = q._n1._op;
			q._n2 = q._n1._n2;
			q._t2 = q._n1._t2;
			q._t1 = q._n1._t1;
			q._n1 = q._n1._n1;
			q.relateToChildren();
		}
		//check unary nodes if they are not root / pull down leaf-unaries
		if (q.isUnary() && q._p != null) {
			if (q._p._n1 == q) {
				q._p._n1 = q._n1;
				q._p._t1 = q._t1;
				if (q._n1 != null) {
					q._n1._p = q._p;
				}
			} else {
				q._p._n2 = q._n1;
				q._p._t2 = q._t1;
				if (q._n2 != null) {
					q._n2._p = q._p;
				}
			}
		}
		if (q._n1 != null) {
			stripUnaryNodes(q._n1);
		}
		if (q._n2 != null) {
			stripUnaryNodes(q._n2);
		}
	}
}
