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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.print.attribute.standard.Finishings;

import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParser.COMP_OP;
import org.zoodb.internal.query.QueryParser.LOG_OP;
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
public final class QueryParserV2 {

	static final Object NULL = new Object();
	
	private int pos = 0;
	private String str;  //TODO final
	private final ZooClassDef clsDef;
	private final Map<String, ZooFieldDef> fields;
	private final  List<QueryParameter> parameters;
	private final List<Pair<ZooFieldDef, Boolean>> order;
	private ArrayList<Token> tokens;
	private int tPos = 0;
	
	public QueryParserV2(String query, ZooClassDef clsDef, List<QueryParameter> parameters,
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
	
	private Token token() {
		return tokens.get(tPos);
	}
	
	private Token token(int i) {
		return tokens.get(tPos + i);
	}
	
	private void tInc() {
		tPos++;
	}
	
	private void tInc(int i) {
		tPos += i;
	}
	
	private boolean match(T_TYPE type) {
		Token t = token();
		return (type == t.type || type.matches(t));
	}
	
	private boolean match(int offs, T_TYPE type) {
		Token t = token(offs);
		return (type == t.type || type.matches(t));
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
		tokens = tokenize(str);
		if (match(T_TYPE.J_SELECT)) {
			tInc();
		}
		if (match(T_TYPE.J_UNIQUE)) {
			//TODO set UNIQUE!
			tInc();
		}
		if (match(T_TYPE.J_FROM)) {
			tInc();
		}
		if (!clsDef.getClassName().equals(token().str)) {
			//TODO class name
			throw DBLogger.newUser("Class mismatch: " + token().pos + " / " + clsDef);
		}
		tInc();
		if (match(T_TYPE.J_WHERE)) {
			tInc();
		}
		
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
		while (match(T_TYPE.NOT)) {
			negate = !negate;
			tInc();
		}
		QueryTerm qt1 = null;
		QueryTreeNode qn1 = null;
		if (match(T_TYPE.OPEN)) {
			tInc();
			qn1 = parseTree(negate);
		} else {
			qt1 = parseTerm(negate);
		}

		if (isFinished()) {
			return new QueryTreeNode(qn1, qt1, null, null, null, negate).relateToChildren();
		}
		
		return parseTree(qt1, qn1, negate);
	}
	
	private QueryTreeNode parseTree(QueryTerm qt1, QueryTreeNode qn1, boolean negate) {
		//parse log op
        if (match(T_TYPE.CLOSE)) {
            tInc();
            if (qt1 == null) {
                return qn1;
            } else {
                return new QueryTreeNode(qn1, qt1, null, null, null, negate);
            }
        }
		char c2 = charAt(1);
		LOG_OP op = null;
        if (match(T_TYPE.AND)) {
			tInc();
			op = LOG_OP.AND;
		} else if (match(T_TYPE.OR)) {
			tInc();
            op = LOG_OP.OR;
		} else if (match(T_TYPE.PARAMETERS)) {
			tInc();
			parseParameters();
			if (qt1 == null) {
				return qn1;
			} else {
				return new QueryTreeNode(qn1, qt1, null, null, null, negate);
			}
		} else if (match(T_TYPE.VARIABLES)) {
			throw new UnsupportedOperationException("JDO feature not supported: VARIABLES");
		} else if (match(T_TYPE.IMPORTS)) {
			throw new UnsupportedOperationException("JDO feature not supported: IMPORTS");
		} else if (match(T_TYPE.GROUP) && match(1, T_TYPE.BY)) {
			throw new UnsupportedOperationException("JDO feature not supported: GROUP BY");
		} else if (match(T_TYPE.ORDER) && match(1, T_TYPE.BY)) {
			tInc(2);
			parseOrdering(str, pos, order, fields);
			return qn1;
			//TODO this fails if ORDER BY is NOT the last part of the query...
		} else if (match(T_TYPE.RANGE)) {
			throw new UnsupportedOperationException("JDO feature not supported: RANGE");
		} else {
			//throw DBLogger.newUser("Unexpected characters: '" + c + c2 + c3 + "' at: " + pos());
			throw DBLogger.newUser("Unexpected characters: '" + str.substring(pos, 
					pos+3 < str.length() ? pos+3 : str.length()) + "' at: " + pos() + 
					"  query=" + str);
		}

		//check negations
		boolean negateNext = negate;
		while (match(T_TYPE.NOT)) {
			negateNext = !negateNext;
			tInc();
		}
		
		// read next term
		QueryTerm qt2 = null;
		QueryTreeNode qn2 = null;
		if (match(T_TYPE.OPEN)) {
			tInc();
			qn2 = parseTree(negateNext);
		} else {
			qt2 = parseTerm(negateNext);
		}

		return new QueryTreeNode(qn1, qt1, op, qn2, qt2, negate);
	}

	private QueryTerm parseTerm(boolean negate) {
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

		ZooFieldDef f = fields.get(fName);
		if (f == null) {
			throw DBLogger.newUser(
					"Field name not found: '" + fName + "' in " + clsDef.getClassName());
		}
		try {
			type = f.getJavaType();
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
		inc( /*op._len*/ );
		trim();
		pos0 = pos();
	
		//read value
		c = charAt0();
		if ((len() >= 4 && substring(pos0, pos0+4).equals("null")) &&
				(len() == 4 || (len()>4 && (charAt(4) == ' ' || charAt(4) == ')')))) {  //hehehe :-)
			if (type.isPrimitive()) {
				throw DBLogger.newUser("Cannot compare 'null' to primitive at pos:" + pos0);
			}
			value = NULL;
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
			while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c=='_')) {
				inc();
				if (isFinished()) break;
				c = charAt0();
			}
			paramName = substring(pos0, pos());
			if (isImplicit) {
				addParameter(type.getName(), paramName);
			} else {
				addParameter(null, paramName);
			}
		}
		if (fName == null || (value == null && paramName == null) || op == null) {
			throw DBLogger.newUser("Cannot parse query at " + pos() + ": " + str);
		}
		trim();
		
		return new QueryTerm(op, paramName, value, clsDef.getField(fName), negate);
	}

	private void parseParameters() {
		while (!isFinished()) {
			char c = charAt0();
			int pos0 = pos;
			while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c=='_') || (c=='.')) {
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
			while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c=='_')) {
				inc();
				if (isFinished()) break;
				c = charAt0();
			}
			String paramName = substring(pos0, pos());
			updateParameterType(typeName, paramName);
			trim();
		}
	}
	
	private void addParameter(String type, String name) {
		for (QueryParameter p: parameters) {
			if (p.getName().equals(name)) {
				throw DBLogger.newUser("Duplicate parameter name: " + name);
			}
		}
		this.parameters.add(new QueryParameter(type, name));
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
			List<Pair<ZooFieldDef, Boolean>> ordering, Map<String, ZooFieldDef> fields) {
		
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
	
	private static enum T_TYPE {
		AND, OR, NOT,
		B_AND, B_OR, B_NOT,
		EQ, NE, LE, GE, L, G,
		PLUS, MINUS, MUL, DIV, MOD,
		OPEN, CLOSE, // ( + )
		COMMA, DOT, COLON, QUOTE, DQUOTE,
		AVG, SUM, MIN, MAX, COUNT,
		PARAM, PARAM_IMPLICIT,
		J_SELECT("SELECT"), J_UNIQUE("UNIQUE"), J_INTO, J_FROM("FROM"), J_WHERE("WHERE"), 
		J_ASC, J_DESC, J_TO, 
		PARAMETERS("PARAMETERS"), VARIABLES(), IMPORTS(), GROUP(), ORDER(), BY(), RANGE(),
		J_STR_STARTS_WITH, J_STR_ENDSWITH,
		J_COL_CONTAINS,
		F_NAME, STRING, NUMBER, TRUE, FALSE, NULL,
		LETTERS; //fieldName, paramName, JDOQL keyword
		
		private final String str;
		T_TYPE(String str) {
			this.str = str;
		}
		T_TYPE() {
			this.str = this.name();
		}
		public boolean matches(Token token) {
			if (token.str != null && token.str.toUpperCase().equals(str)) {
				return true;
			}
			return false;
		}
	}
	
	private static class Token {
		final T_TYPE type;
		final String str;
		final int pos;
		public Token(T_TYPE type, String str, int pos) {
			this.type = type;
			this.str = str;
			this.pos = pos;
		}
		public Token(T_TYPE type, int pos) {
			this(type, null, pos);
		}
		@Override
		public String toString() {
			return type.name() + " - \"" + str + "\" -- " + pos;
		}
	}
	
	public ArrayList<Token> tokenize(String query) {
		ArrayList<Token> r = new ArrayList<>();
		pos = 0;
		str = query;
		while (pos() < query.length()) {
			skipWS();
			if (isFinished()) {
				break;
			}
			r.add( readToken() );
		}
		return r;
	}
	
	private Token readToken() {

		Token t = null;
		//first: check single-chars
		char c = charAt0();
		switch (c) {
		case '(': t = new Token(T_TYPE.OPEN, pos); break;  
		case ')': t = new Token(T_TYPE.CLOSE, pos); break;
		case ',': t = new Token(T_TYPE.COMMA, pos); break;
		case ':': t = new Token(T_TYPE.COLON, pos); break;
		case '.': t = new Token(T_TYPE.DOT, pos); break;
		case '~': t = new Token(T_TYPE.B_NOT, pos); break;
		case '+': t = new Token(T_TYPE.PLUS, pos); break;
//		case '-': t = new Token(T_TYPE.MINUS, pos); break;
		case '*': t = new Token(T_TYPE.MUL, pos); break;
		case '/': t = new Token(T_TYPE.DIV, pos); break;
		case '%': t = new Token(T_TYPE.MOD, pos); break;
//		case '&': t = new Token(TOKEN.B_AND, pos); break;
//		case '|': t = new Token(TOKEN.B_OR, pos); break;
//		case '!': t = new Token(TOKEN.NOT, pos); break;
//		case '>': t = new Token(TOKEN.G, pos); break;
//		case '<': t = new Token(TOKEN.L, pos); break;
		}
		if (t != null) {
			inc();
			return t;
		}
		if (isFinished()) {
			throw DBLogger.newUser("Error parsing query at pos " + pos + ": " + str);
		}
		
		char c2 = charAt(1);
		String tok = "" + c + c2;
		switch (tok) {
		case "&&": t = new Token(T_TYPE.AND, pos); break;  
		case "||": t = new Token(T_TYPE.OR, pos); break;
		case "==" : t = new Token(T_TYPE.EQ, pos); break;
		case "!=": t = new Token(T_TYPE.COLON, pos); break;
		case "<=" : t = new Token(T_TYPE.COMMA, pos); break;
		case ">=": t = new Token(T_TYPE.COLON, pos); break;
		}
		if (t != null) {
			inc(2);
			return t;
		}

		//separate check to avoid collision with 2-chars
		switch (c) {
		case '&': t = new Token(T_TYPE.B_AND, pos); break;
		case '|': t = new Token(T_TYPE.B_OR, pos); break;
		case '!': t = new Token(T_TYPE.NOT, pos); break;
		case '>': t = new Token(T_TYPE.G, pos); break;
		case '<': t = new Token(T_TYPE.L, pos); break;
		}
		if (t != null) {
			inc(1);
			return t;
		}

		if (isFinished()) {
			throw DBLogger.newUser("Error parsing query at pos " + pos + ": " + str);
		}
		
		if ((c=='-' && c2 >= '0' && c2 <= '9') || (c >= '0' && c <= '9')) {
			return parseNumber();
		}
		
		if (c=='-') {
			inc();
			return new Token(T_TYPE.MINUS, pos);
		}

		
		if (c=='"' || c=='\'') {
			return parseString();
		}
		if (c==':') {
			return parseImplicitParam();
		}
		if (c=='t' || c=='T' || c=='F' || c=='F') {
			t = parseBoolean();
			if (t != null) {
				return t;
			}
		}
		if (c=='n' || c=='N') {
			t = parseNull();
			if (t != null) {
				return t;
			}
		}
		
		return parseFieldOrParam();
	}

	private Token parseNumber() {
		char c = charAt0();
		String v = "";
		if (c=='-') {
			v += c;
			pos++;
		}
		
		int pos0 = pos();
		int base = 10;
		
		while (!isFinished()) {
			c = charAt0();
			if ((c >= '0' && c <= '9') || c=='x' || c=='b' || c==',' || c=='.' || 
					c=='L' || c=='l' || c=='F' || c=='f') {
				switch (c) {
				case 'x' : base = 16; break;
				case 'b' : base = 2; break;
				}
				v += c;
				inc();
				continue;
			}
			break;
		}
		return new Token(T_TYPE.NUMBER, v, pos0);
	}
	
	private Token parseString() {
		//According to JDO 2.2 14.6.2, String and single characters can both be delimited by 
		//both single and double quotes.
		char c = charAt0();
		boolean singleQuote = c == '\''; 
		inc();
		int pos0 = pos();
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
		String value = substring(pos0, pos());
		Token t = new Token(T_TYPE.STRING, value, pos0);
		inc();
		return t;
	}
	
	private Token parseImplicitParam() {
		inc();
		int pos0 = pos();
		char c = charAt0();
		
		while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c=='_')) {
			inc();
			if (isFinished()) break;
			c = charAt0();
		}
		String paramName = substring(pos0, pos());
		return new Token(T_TYPE.PARAM_IMPLICIT, paramName, pos0);
	}

	private Token parseFieldOrParam() {
		int pos0 = pos();
		char c = charAt0();
		
		while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || 
				(c=='_')) {
			inc();
			if (isFinished()) break;
			c = charAt0();
		}
		String paramName = substring(pos0, pos());
		return new Token(T_TYPE.LETTERS, paramName, pos0);
	}

	private Token parseBoolean() {
		int pos0 = pos();
		if (substring(pos0, pos0+4).toLowerCase().equals("true") 
				&& (isFinished(4) || isWS(charAt(4)) || charAt(4)==')')) {
			inc(4);
			return new Token(T_TYPE.TRUE, pos0);
		} else if (substring(pos0, pos0+5).toLowerCase().equals("false") 
				&& (isFinished(5) || isWS(charAt(5)) || charAt(5)==')')) {
			inc(5);
			return new Token(T_TYPE.FALSE, pos0);
		}
		return null;  //not a boolean...
	}
	
	private Token parseNull() {
		int pos0 = pos();
		if (substring(pos0, pos0+4).toLowerCase().equals("null") 
				&& (isFinished(4) || isWS(charAt(4)) || charAt(4)==')')) {
			inc(4);
			return new Token(T_TYPE.NULL, pos0);
		}
		return null;  //not a null...
	}
	
	private void skipWS() {
		while (isWS(charAt0()) && !isFinished()) {
			inc();
		}
		return;
	}
}