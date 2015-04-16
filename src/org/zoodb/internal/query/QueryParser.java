/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.internal.query;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Pair;


/**
 * The query parser. This class builds a query tree from a query string.
 * The tree consists of QueryTerms (comparative statements) and QueryNodes (logical operations on
 * two children (QueryTerms or QueryNodes).
 * The root of the tree is a QueryNode. QueryNodes may have only a single child.  
 * 
 * Negation is implemented by simply negating all operators inside the negated term.
 * 
 * TODO QueryOptimiser:
 * E.g. "((( A==B )))"Will create something like Node->Node->Node->Term. Optimise this to 
 * Node->Term. That means pulling up all terms where the parent node has no other children. The
 * only exception is the root node, which is allowed to have only one child.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryParser {

	private int pos = 0;
	private final String str;
	private final ZooClassDef clsDef;
	private final Map<String, ZooFieldDef> fields;
	private final  List<QueryParameter> parameters;
	private final List<Pair<ZooFieldDef, Boolean>> order;
	
	public QueryParser(String query, ZooClassDef clsDef, List<QueryParameter> parameters,
			List<Pair<ZooFieldDef, Boolean>> order) {
		this.str = query; 
		this.clsDef = clsDef;
		this.fields = clsDef.getAllFieldsAsMap();
		this.parameters = parameters;
		this.order = order;
	}
	
	private void trim() {
		while (!isFinished() && isWS(charAt0())) {
			pos++;
		}
	}
	
	/**
	 * @param c
	 * @return true if c is a whitespace character
	 */
	private static boolean isWS(char c) {
		return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f';
	}
	
	private char charAt0() {
		return str.charAt(pos);
	}
	
	private char charAt(int i) {
		return str.charAt(pos + i);
	}
	
	private void inc() {
		pos++;
	}
	
	private void inc(int i) {
		pos += i;
	}
	
	private int pos() {
		return pos;
	}
	
	private boolean isFinished() {
		return !(pos < str.length());
	}
	
	/**
	 * 
	 * @param ofs
	 * @return Whether the string is finished after the givven offset
	 */
	private boolean isFinished(int ofs) {
		return !(pos + ofs < str.length());
	}
	
	/**
	 * @return remaining length.
	 */
	private int len() {
		return str.length() - pos;
	}

	
	/**
	 * 
	 * @param pos0 start, absolute position, inclusive
	 * @param pos1 end, absolute position, exclusive
	 * @return sub-String
	 */
	private String substring(int pos0, int pos1) {
		if (pos1 > str.length()) {
			throw DBLogger.newUser("Unexpected end of query: '" + str.substring(pos0, 
					str.length()) + "' at: " + pos() + "  query=" + str);
		}
		return str.substring(pos0, pos1);
	}
	
	public QueryTreeNode parseQuery() {
		//Negation is used to invert negated operand.
		//We just pass it down the tree while parsing, always inverting the flag if a '!' is
		//encountered. When popping out of a function, the flag is reset to the value outside
		//the term that was parsed in a function. Actually, it is not reset, it is never modified.
		boolean negate = false;
		QueryTreeNode qn = parseTree(negate);
		while (!isFinished()) {
			qn = parseTree(null, qn, negate);
		}
		return qn;
	}
	
	private QueryTreeNode parseTree(boolean negate) {
		trim();
		while (charAt0() == '!') {
			negate = !negate;
			inc(LOG_OP.NOT._len);
			trim();
		}
		QueryTerm qt1 = null;
		QueryTreeNode qn1 = null;
		if (charAt0() == '(') {
			inc();
			qn1 = parseTree(negate);
			trim();
		} else {
			qt1 = parseTerm(negate);
		}

		if (isFinished()) {
			return new QueryTreeNode(qn1, qt1, null, null, null, negate).relateToChildren();
		}
		
		return parseTree(qt1, qn1, negate);
	}
	
	private QueryTreeNode parseTree(QueryTerm qt1, QueryTreeNode qn1, boolean negate) {
		trim();

		//parse log op
		char c = charAt0();
        if (c == ')') {
            inc(1);
            trim();
            if (qt1 == null) {
                return qn1;
            } else {
                return new QueryTreeNode(qn1, qt1, null, null, null, negate);
            }
        }
		char c2 = charAt(1);
		LOG_OP op = null;
        if (c == '&' && c2 ==  '&') {
			op = LOG_OP.AND;
		} else if (c == '|' && c2 ==  '|') {
            op = LOG_OP.OR;
		} else if (substring(pos, pos+10).toUpperCase().equals("PARAMETERS")) {
			inc(10);
			trim();
			parseParameters();
			if (qt1 == null) {
				return qn1;
			} else {
				return new QueryTreeNode(qn1, qt1, null, null, null, negate);
			}
		} else if (substring(pos, pos+9).toUpperCase().equals("VARIABLES")) {
			throw new UnsupportedOperationException("JDO feature not supported: VARIABLES");
		} else if (substring(pos, pos+7).toUpperCase().equals("IMPORTS")) {
			throw new UnsupportedOperationException("JDO feature not supported: IMPORTS");
		} else if (substring(pos, pos+8).toUpperCase().equals("GROUP BY")) {
			throw new UnsupportedOperationException("JDO feature not supported: GROUP BY");
		} else if (substring(pos, pos+8).toUpperCase().equals("ORDER BY")) {
			inc(8);
			parseOrdering(str, pos, order, clsDef);
			pos = str.length(); //isFinished()!
			return qn1;
			//TODO this fails if ORDER BY is NOT the last part of the query...
		} else if (substring(pos, pos+5).toUpperCase().equals("RANGE")) {
			throw new UnsupportedOperationException("JDO feature not supported: RANGE");
		} else {
			//throw DBLogger.newUser("Unexpected characters: '" + c + c2 + c3 + "' at: " + pos());
			throw DBLogger.newUser("Unexpected characters: '" + str.substring(pos, 
					pos+3 < str.length() ? pos+3 : str.length()) + "' at: " + pos() + 
					"  query=" + str);
		}
		inc( op._len );
		trim();

		//check negations
		boolean negateNext = negate;
		while (charAt0() == '!') {
			negateNext = !negateNext;
			inc(LOG_OP.NOT._len);
			trim();
		}
		
		// read next term
		QueryTerm qt2 = null;
		QueryTreeNode qn2 = null;
		if (charAt0() == '(') {
			inc();
			qn2 = parseTree(negateNext);
			trim();
		} else {
			qt2 = parseTerm(negateNext);
		}

		return new QueryTreeNode(qn1, qt1, op, qn2, qt2, negate);
	}

	private QueryTerm parseTerm(boolean negate) {
		trim();
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
//						throw new JDOUserException("Cannot parse query at position " + pos0 + 
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
			throw DBLogger.newUser("Cannot parse query at position " + pos0 + ": '" + c +"'");
		}
		pos0 = pos();
		trim();

		ZooFieldDef fieldDef = fields.get(fName);
		if (fieldDef == null) {
			throw DBLogger.newUser(
					"Field name not found: '" + fName + "' in " + clsDef.getClassName());
		}
		try {
			type = fieldDef.getJavaType();
			if (type == null) {
				throw DBLogger.newUser(
						"Field name not found: '" + fName + "' in " + clsDef.getClassName());
			}
		} catch (SecurityException e) {
			throw DBLogger.newUser("Field not accessible: " + fName, e);
		}


		//read operator
		c = charAt0();
		char c2 = charAt(1);
		char c3 = charAt(2);
		if (c == '=' && c2 ==  '=') {
			op = COMP_OP.EQ;
		} else if (c == '<') {
			if (c2 == '=') {
				op = COMP_OP.LE;
			} else {
				op = COMP_OP.L;
			}
		} else if (c == '>') {
			if (c2 ==  '=') {
				op = COMP_OP.AE;
			} else {
				op = COMP_OP.A;
			}
		} else if (c == '!' && c2 == '=') {
			op = COMP_OP.NE;
		}
		if (op == null) {
			throw DBLogger.newUser("Unexpected characters: '" + c + c2 + c3 + "' at: " + pos0);
		}
		inc( op._len );
		trim();
		pos0 = pos();
	
		//read value
		c = charAt0();
		if ((len() >= 4 && substring(pos0, pos0+4).equals("null")) &&
				(len() == 4 || (len()>4 && (charAt(4) == ' ' || charAt(4) == ')')))) {  //hehehe :-)
			if (type.isPrimitive()) {
				throw DBLogger.newUser("Cannot compare 'null' to primitive at pos:" + pos0);
			}
			value = QueryTerm.NULL;
			inc(4);
		} else if (c=='"' || c=='\'') {
			//According to JDO 2.2 14.6.2, String and single characters can both be delimited by 
			//both single and double quotes.
			boolean singleQuote = c == '\''; 
			//TODO allow char type!
			if (!String.class.isAssignableFrom(type)) {
				throw DBLogger.newUser("Incompatible types, found String, expected: " + 
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
					if (isFinished(pos()+1)) {
						throw DBLogger.newUser("Try using \\\\\\\\ for double-slashes.");
					}
				}					
				inc();
				c = charAt0();
			}
			value = substring(pos0, pos());
			inc();
		} else if (c=='-' || (c >= '0' && c <= '9')) {
			pos0 = pos();
			boolean isHex = false;
			while (!isFinished()) {
				c = charAt0();
				if (c==')' || isWS(c) || c=='|' || c=='&') {
					break;
//				} else if (c=='.') {
//					isDouble = true;
//				} else if (c=='L' || c=='l') {
//					//if this is not at the last position, then we will fail later anyway
//					isLong = true;
				} else if (c=='x') {
					//if this is not at the second position, then we will fail later anyway
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
					value = Long.parseLong( substring(pos0+2, pos()), 16 );
				} else {
					value = Long.parseLong( substring(pos0, pos()));
				}
			} else if (type == Integer.TYPE || type == Integer.class) {
				if (isHex) {
					value = Integer.parseInt( substring(pos0+2, pos()), 16 );
				} else {
					value = Integer.parseInt( substring(pos0, pos()) );
				}
			} else if (type == Short.TYPE || type == Short.class) {
				if (isHex) {
					value = Short.parseShort( substring(pos0+2, pos()), 16 );
				} else {
					value = Short.parseShort( substring(pos0, pos()) );
				}
			} else if (type == Byte.TYPE || type == Byte.class) {
				if (isHex) {
					value = Byte.parseByte( substring(pos0+2, pos()), 16 );
				} else {
					value = Byte.parseByte( substring(pos0, pos()) );
				}
			} else if (type == BigDecimal.class) {
				value = new BigDecimal( substring(pos0+2, pos()) );
			} else if (type == BigInteger.class) {
				value = new BigInteger( substring(pos0, pos()) );
			} else { 
				throw DBLogger.newUser("Incompatible types, found number, expected: " +	type);
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
			throw DBLogger.newUser("Incompatible types, expected Boolean, found: " + 
					substring(pos0, pos0+5));
			}
		} else {
			boolean isImplicit = false;
			if (c==':') {
				//implicit paramter
				isImplicit = true;
				inc();
				pos0 = pos;
				c = charAt0();
			}
			while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || 
					(c=='_')) {
				inc();
				if (isFinished()) break;
				c = charAt0();
			}
			paramName = substring(pos0, pos());
			if (isImplicit) {
				addParameter(type.getName(), paramName, fieldDef.isPrimitiveType());
			} else {
				addParameter(null, paramName, false);
			}
		}
		if (fName == null || (value == null && paramName == null) || op == null) {
			throw DBLogger.newUser("Cannot parse query at " + pos() + ": " + str);
		}
		trim();
		
		return new QueryTerm(fieldDef, op, paramName, value, null, negate);
	}

	static enum COMP_OP {
		EQ(false, false, true), 
		NE(true, true, false), 
		LE(true, false, true), 
		AE(false, true, true), 
		L(true, false, false), 
		A(false, true, false),
		COLL_contains(Object.class), COLL_isEmpty(), COLL_size(),
		MAP_containsKey(Object.class), MAP_isEmpty(), MAP_size(),
		MAP_containsValue(Object.class), MAP_get(Object.class),
		LIST_get(Integer.TYPE),
		STR_startsWith(String.class), STR_endsWith(String.class),
		STR_indexOf1(String.class), STR_indexOf2(String.class, Integer.TYPE),
		STR_substring1(Integer.TYPE), STR_substring2(Integer.TYPE, Integer.TYPE),
		STR_toLowerCase(), STR_toUpperCase(),
		STR_matches(String.class), STR_contains_NON_JDO(String.class),
		JDOHelper_getObjectId(Object.class),
		Math_abs(Number.class), Math_sqrt(Number.class);

		private final boolean isComparator;
		private final Class<?>[] args;
		private final int _len;
        private final boolean _allowsLess;
        private final boolean _allowsMore;
        private final boolean _allowsEqual;

		private COMP_OP(Class<?> ... args) {
			this.isComparator = false; 
			this.args = args;
			_len = -1;
            _allowsLess = false; 
            _allowsMore = false; 
            _allowsEqual = false; 
		}
		private COMP_OP(boolean al, boolean am, boolean ae) {
			_len = name().length();
            _allowsLess = al; 
            _allowsMore = am; 
            _allowsEqual = ae;
            isComparator = true;
            this.args = new Class<?>[]{};
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
        
        COMP_OP inverstIfTrue(boolean inverse) {
        	if (!inverse) {
        		return this;
        	}
        	switch (this) {
        	case EQ: return NE;
        	case NE: return EQ;
        	case LE: return A;
        	case AE: return L;
        	case L: return AE;
        	case A: return LE;
        	default: throw new IllegalArgumentException();
        	}
        }
        
        boolean isComparator() {
        	return isComparator;
        }
		public int argCount() {
			return args.length;
		}
	}

	static enum FNCT_OP {
		CONSTANT(),
		REF(),
		FIELD(),
		THIS(),
		
		COLL_contains(Object.class), COLL_isEmpty(), COLL_size(),
		MAP_containsKey(Object.class), MAP_isEmpty(), MAP_size(),
		MAP_containsValue(Object.class), MAP_get(Object.class),
		LIST_get(Integer.TYPE),
		STR_startsWith(String.class), STR_endsWith(String.class),
		STR_indexOf1(String.class), STR_indexOf2(String.class, Integer.TYPE),
		STR_substring1(Integer.TYPE), STR_substring2(Integer.TYPE, Integer.TYPE),
		STR_toLowerCase(), STR_toUpperCase(),
		STR_matches(String.class), STR_contains_NON_JDO(String.class),
		JDOHelper_getObjectId(Object.class),
		Math_abs(Number.class), Math_sqrt(Number.class);

		private final Class<?>[] args;

		private FNCT_OP(Class<?> ... args) {
			this.args = args;
		}
        
		public int argCount() {
			return args.length;
		}
        
		public Class<?>[] args() {
			return args;
		}
	}

	/**
	 * Logical operators.
	 */
	enum LOG_OP {
		AND(2), // && 
		OR(2),  // ||
		//XOR(2);  
		NOT(1);  //TODO e.g. not supported in unary-stripper or in index-advisor

		private final int _len;

		private LOG_OP(int len) {
			_len = len;
		}
		LOG_OP inverstIfTrue(boolean inverse) {
			if (!inverse) {
				return this;
			}
			switch (this) {
			case AND: return OR;
			case OR: return AND;
			default: throw new IllegalArgumentException();
			}
		}
	}

	private void parseParameters() {
		while (!isFinished()) {
			char c = charAt0();
			int pos0 = pos;
			while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || 
					(c=='_') || (c=='.')) {
				inc();
				if (isFinished()) break;
				c = charAt0();
			}
			String typeName = substring(pos0, pos());
	
			//TODO check here for
			//IMPORTS
			//GROUP_BY
			//ORDER_BY
			//RANGE
			//TODO .. and implement according sub-methods
			
			trim();
			c = charAt0();
			pos0 = pos;
			while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || 
					(c=='_')) {
				inc();
				if (isFinished()) break;
				c = charAt0();
			}
			String paramName = substring(pos0, pos());
			updateParameterType(typeName, paramName);
			trim();
		}
	}
	
	private void addParameter(String type, String name, boolean isPC) {
		for (QueryParameter p: parameters) {
			if (p.getName().equals(name)) {
				throw DBLogger.newUser("Duplicate parameter name: " + name);
			}
		}
		this.parameters.add(new QueryParameter(type, name, isPC));
	}
	
	private void updateParameterType(String type, String name) {
		for (QueryParameter p: parameters) {
			if (p.getName().equals(name)) {
				if (p.getType() != null) {
					throw DBLogger.newUser("Duplicate parameter name: " + name);
				}
				p.setType(type);
				return;
			}
		}
		throw DBLogger.newUser("Parameter not used in query: " + name);
	}

	
	public static void parseOrdering(final String input, int pos, 
			List<Pair<ZooFieldDef, Boolean>> ordering, ZooClassDef candClsDef) {
		Map<String, ZooFieldDef> fields = candClsDef.getAllFieldsAsMap();
		
		ordering.clear();

		if (input == null) {
			return;
		}
		String orderingStr = input.substring(pos).trim();
		while (orderingStr.length() > 0) {
			int p = orderingStr.indexOf(' ');
			if (p < 0) {
				throw DBLogger.newUser("Parse error near position " + pos + "  input=" + input);
			}
			String attrName = orderingStr.substring(0, p).trim();
			pos += attrName.length()+1;
			ZooFieldDef f = fields.get(attrName);
			if (f == null) {
				throw DBLogger.newUser("Field '" + attrName + "' not found at position " + pos);
			}
			if (!f.isPrimitiveType() && !f.isString()) {
				throw DBLogger.newUser("Field not sortable: " + f);
			}
			for (Pair<ZooFieldDef, Boolean> p2: ordering) {
				if (p2.getA().equals(f)) {
					throw DBLogger.newUser("Parse error, field '" + f + "' is sorted twice near "
							+ "position " + pos + "  input=" + input);
				}
			}
			
			orderingStr = orderingStr.substring(p).trim();
			int d;
			if (orderingStr.toUpperCase().startsWith("ASC")) {
				if (orderingStr.toUpperCase().startsWith("ASCENDING")) {
					d = 9;
				} else {
					d = 3;
				}
				ordering.add(new Pair<ZooFieldDef, Boolean>(f, true));
			} else if (orderingStr.toUpperCase().startsWith("DESC")) {
				if (orderingStr.toUpperCase().startsWith("DESCENDING")) {
					d = 10;
				} else {
					d = 4;
				}
				ordering.add(new Pair<ZooFieldDef, Boolean>(f, false));
			} else {
				throw DBLogger.newUser("Parse error at position " + pos);
			}
			pos += d;
			orderingStr = orderingStr.substring(d).trim(); 

			if (orderingStr.length() > 0) {
				if (orderingStr.startsWith(",")) {
					orderingStr = orderingStr.substring(1).trim();
					pos += 1;
					if (orderingStr.length() == 0) {
						throw DBLogger.newUser("Parse error, unexpected end near position " + pos + 
								"  input=" + input);
					}
				} else {
					throw DBLogger.newUser("Parse error, expected ',' near position " + pos + 
							"  input=" + input);
				}
			}
		}
	}
}