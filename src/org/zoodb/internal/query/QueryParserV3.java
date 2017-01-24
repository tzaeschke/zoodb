/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParameter.DECLARATION;
import org.zoodb.internal.query.QueryParser.COMP_OP;
import org.zoodb.internal.query.QueryParser.FNCT_OP;
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
 * E.g. "((( A==B )))"Will create something like Node(Node(Node(Term))). Optimize this to 
 * Node(Term). That means pulling up all terms where the parent node has no other children. The
 * only exception is the root node, which is allowed to have only one child.
 * 
 * There are different parsers available. They differ in the query features they can parse but also
 * in the parsing speed. Lower version understand usually less but parse faster. 
 * TODO potentially we could start all parsers in parallel and use the output of the parser that
 * returns first without error (i.e. manages to parse the query).
 *  
 * V1 is a simple parser that does not understand functions etc
 * V2 also understands boolean functions
 * V3 also understands paths and references.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryParserV3 {

	private int pos = 0;
	private String str;  //TODO final
	private final ZooClassDef clsDef;
	private final Map<String, ZooFieldDef> fields;
	private final List<QueryParameter> parameters;
	private final List<Pair<ZooFieldDef, Boolean>> order;
	private ArrayList<Token> tokens;
	private int tPos = 0;
	private final QueryFunction THIS;
	
	private long rangeMin;
	private long rangeMax;
	private QueryParameter rangeMinParam = null;
	private QueryParameter rangeMaxParam = null;
	
	public QueryParserV3(String query, ZooClassDef clsDef, List<QueryParameter> parameters,
			List<Pair<ZooFieldDef, Boolean>> order, long rangeMin, long rangeMax) {
		this.str = query; 
		this.clsDef = clsDef;
		this.fields = clsDef.getAllFieldsAsMap();
		this.parameters = parameters;
		this.order = order;
		this.THIS = QueryFunction.createThis(clsDef);
		this.rangeMin = rangeMin;
		this.rangeMax = rangeMax;
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
		try {
			return tokens.get(tPos);
		} catch (IndexOutOfBoundsException e) {
			throw tokenParsingError("Unexpected end");
		}
	}
	
	private Token token(int i) {
		try {
			return tokens.get(tPos + i);
		} catch (IndexOutOfBoundsException e) {
			throw tokenParsingError("Unexpected end");
		}
	}
	
	private RuntimeException tokenParsingError(String msg) {
		int tp = tPos < tokens.size() ? tPos : tokens.size()-1;
		Token t = tokens.get(tp);
		return DBLogger.newUser("Query parsing error at '" + t.msg() + 
				"'  near position " + t.pos + ": " + msg + ". Query= " + str);
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
	
	private boolean hasMoreTokens() {
		return tPos < tokens.size();
	}
	
	private boolean hasMoreTokens(int offset) {
		return tPos+offset < tokens.size();
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
	 * @return Whether the string is finished after the given offset
	 */
	private boolean isFinished(int ofs) {
		return !(pos + ofs < str.length());
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
					str.length()) + "' near position: " + pos() + "  query= " + str);
		}
		return str.substring(pos0, pos1);
	}
	
	public QueryTreeNode parseQuery() {
		try {
			tokens = tokenize(str);
		} catch (StringIndexOutOfBoundsException e) {
			throw DBLogger.newUser("Parsing error: unexpected end at position " + pos + 
					". Query= " + str);
		}
		
		try {
			if (match(T_TYPE.SELECT)) {
				if (!isTokenFieldName()) {
					//skip SELECT
					tInc();
				}
				//else: must be a field called 'select'.
			}
			if (match(T_TYPE.UNIQUE)) {
				//skip for fields named 'unique'
				if (!isTokenFieldName()) {
					//TODO set UNIQUE!
					tInc();
				}
			}
			if (match(T_TYPE.FROM)) {
				//skip for fields named 'from'
				if (!isTokenFieldName()) {
					tInc();
					if (!clsDef.getClassName().equals(token().str)) {
						//TODO class name
						throw DBLogger.newUser("Class mismatch: " + token().pos + " / " + clsDef);
					}
					tInc();
				}
			}
			if (match(T_TYPE.WHERE)) {
				//skip for fields named 'where'
				if (!isTokenFieldName()) {
					tInc();
				}
			}
			
			//Negation is used to invert negated operand.
			//We just pass it down the tree while parsing, always inverting the flag if a '!' is
			//encountered. When popping out of a function, the flag is reset to the value outside
			//the term that was parsed in a function. Actually, it is not reset, it is never modified.
			boolean negate = false;
			QueryTreeNode qn = parseTree(negate);
			while (hasMoreTokens()) {
				qn = parseTree(null, qn, negate);
			}
			return qn;
		} catch (StringIndexOutOfBoundsException e) {
			throw tokenParsingError("Unexpected end");
		}
	}
	
//	private boolean isNextTokenComparator() {
//		if (!hasMoreTokens()) {
//			return false;
//		}
//		Token t = token(1);
//		switch (t.type) {
//		case EQ:
//		case GE:
//		case G:
//		case LE:
//		case L:
//			return true;
//		default: 
//			return false;
//		}
//	}
	
	private boolean isTokenFieldName() {
		if (!hasMoreTokens()) {
			return false;
		}
		Token t = token(1);
		char c = t.str.charAt(0);
		//if it is followed by a letter than it cannot be field name
		return !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'));
	}
	
	private QueryTreeNode parseTree(boolean negate) {
		while (match(T_TYPE.L_NOT)) {
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

		if (!hasMoreTokens()) {
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

        LOG_OP op = null;
        if (match(T_TYPE.L_AND)) {
			tInc();
			op = LOG_OP.AND;
		} else if (match(T_TYPE.L_OR)) {
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
			parseOrdering();
			if (qt1 == null) {
				return qn1;
			} else {
				return new QueryTreeNode(qn1, qt1, null, null, null, negate);
			}
		} else if (match(T_TYPE.RANGE)) {
			tInc();
			parseRange();
            if (qt1 == null) {
                return qn1;
            } else {
                return new QueryTreeNode(qn1, qt1, null, null, null, negate);
            }
		} else {
			throw tokenParsingError("Found unexpected characters '" + token().msg() + "'");
		}

		//check negations
		boolean negateNext = negate;
		while (match(T_TYPE.L_NOT)) {
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
		//read field name
		if (match(T_TYPE.THIS) && match(1, T_TYPE.DOT)) {
			tInc(2);
		}
		String lhsFName = token().str;

		ZooFieldDef lhsFieldDef = fields.get(lhsFName);
		Class<?> lhsType = null;
		Object lhsValue = null;
		QueryFunction lhsFn = null;
		if (lhsFieldDef == null) {
			if (match(T_TYPE.THIS)) {
				//only 'this' on the left side
				lhsType = clsDef.getJavaClass();
				lhsValue = QueryTerm.THIS;
				tInc();
			} else {
				if (match(T_TYPE.RANGE)) {
					//is this an empty query with a range declaration???
					if (hasMoreTokens() && 
							(match(1, T_TYPE.NUMBER_INT) || match(1, T_TYPE.NUMBER_LONG))) {
						tInc();
						parseRange();
						return new QueryTerm(QueryFunction.createConstant(Boolean.TRUE), negate);
					} else if (hasMoreTokens(3) && 
							match(1, T_TYPE.COLON) && match(3,T_TYPE.COMMA)) {
						tInc();
						parseRange();
						return new QueryTerm(QueryFunction.createConstant(Boolean.TRUE), negate);
					}
				}
				lhsFn = parseFunction(THIS);
				if (!hasMoreTokens() && lhsFn.getReturnType() == Boolean.TYPE) {
					return new QueryTerm(lhsFn, negate);
				}
				lhsType = lhsFn.getReturnType();
			}
		} else {
			try {
				lhsType = lhsFieldDef.getJavaType();
				if (lhsType == null) {
					throw tokenParsingError(
							"Field name not found: '" + lhsFName + "' in " + clsDef.getClassName());
				}
			} catch (SecurityException e) {
				throw DBLogger.newUser("Field not accessible: " + lhsFName, e);
			}
			if (match(1, T_TYPE.DOT)) {
				lhsFn = parseFunction(THIS);
				if (!hasMoreTokens() && lhsFn.getReturnType() == Boolean.TYPE) {
					return new QueryTerm(lhsFn, negate);
				}
				lhsType = lhsFn.getReturnType();
			} else {
				tInc();
			}
		}

		//read operator
		boolean requiresParenthesis = false;
		COMP_OP op = null;
		switch (token().type) {
		case EQ: op = COMP_OP.EQ; break;
		case LE: op = COMP_OP.LE; break;
		case L: op = COMP_OP.L; break;
		case GE: op = COMP_OP.AE; break;
		case G: op = COMP_OP.A; break;
		case NE: op = COMP_OP.NE; break;
		default:
			if (lhsFn != null && lhsFn.getReturnType() == Boolean.TYPE) {
				return new QueryTerm(lhsFn, negate);
			}
			if (lhsFn != null && lhsFn.op() == FNCT_OP.PARAM && match(-1, T_TYPE.RANGE)) {
				//okay, try RANGE
				tInc(-1);
				return null;
				//TODO ORDER BY?
			}
			throw tokenParsingError("Comparator expected");
		}
		if (op == null) {
			throw tokenParsingError("Unexpected token '" + token().msg());
		}
		tInc();
	
		if (match(T_TYPE.THIS)) 
			if (tPos+1 < tokens.size() && match(1, T_TYPE.DOT)) {
			tInc(2);
		}

		//read value
		Object rhsValue = null;
		String rhsParamName = null;
		QueryFunction rhsFn = null;
		ZooFieldDef rhsFieldDef = null;
		//TODO use switch()?
		//First we need to check for STRING, to avoid recognizing "null" as null.
		if (match(T_TYPE.STRING)) {
			//TODO allow char type!
			if (lhsType == null) {
				throw tokenParsingError("Missing left hand side type info");
			}
			if (hasMoreTokens(1) && match(1, T_TYPE.DOT)) {
				rhsFn = parseFunction(QueryFunction.createConstant(token().str));
				if (!(rhsFn.getReturnType().isAssignableFrom(lhsType) || 
						lhsType.isAssignableFrom(rhsFn.getReturnType()))) {
					throw tokenParsingError("Incompatible types, found " +
							rhsFn.getReturnType() + ", expected: " + lhsType.getName());
				}
			} else {
				if (!(String.class.isAssignableFrom(lhsType) || 
						Collection.class.isAssignableFrom(lhsType) || 
						Map.class.isAssignableFrom(lhsType))) {
					throw tokenParsingError(
							"Incompatible types, found 'String', expected: " + lhsType.getName());
				}
				rhsValue = token().str;
				tInc();
			}
		} else if (match(T_TYPE.NULL)) {
			if (lhsType.isPrimitive()) {
				throw tokenParsingError("Cannot compare 'null' to primitive");
			}
			rhsValue = QueryTerm.NULL;
			tInc();
		} else if (match(T_TYPE.NUMBER_INT) || match(T_TYPE.NUMBER_LONG) || 
				match(T_TYPE.NUMBER_FLOAT) || match(T_TYPE.NUMBER_DOUBLE)) {
			rhsValue = tokenToNumber();
			tInc();
		} else if (lhsType == Boolean.TYPE || lhsType == Boolean.class) {
			if (match(T_TYPE.TRUE)) {
				rhsValue = true;
			} else if (match(T_TYPE.FALSE)) {
				rhsValue = false;
			} else {
				rhsFn = parseFunction(THIS);
				if (rhsFn.getReturnType() != Boolean.TYPE) {
					throw tokenParsingError(
							"Incompatible types, expected 'Boolean', found " + token().msg());
				}
			}
			tInc();
		} else if (match(T_TYPE.THIS)) {
			tInc();
			if (hasMoreTokens() && match(T_TYPE.DOT)) {
				rhsFn = parseFunction(THIS);
			}
			rhsValue = QueryTerm.THIS;
		} else {
			boolean isImplicit = match(T_TYPE.COLON);
			if (isImplicit) {
				tInc();
				rhsParamName = token().str;
				if (lhsType == null) {
					throw tokenParsingError("missing left hand side type info");
				}
				addImplicitParameter(lhsType, rhsParamName);
				tInc();
			} else {
				rhsFn = parseFunction(THIS);
			}
		}
		if (rhsValue == null && rhsParamName == null && rhsFieldDef == null && rhsFn == null) {
			throw tokenParsingError("Found \"" + token().str + "\"" + token().type);
		}
		
		if (requiresParenthesis) {
			if (!match(T_TYPE.CLOSE)) {
				throw tokenParsingError("Expected ')' but got: '" + token().str + "'");
			}
			tInc();
		}
		
		return new QueryTerm(lhsValue, lhsFieldDef, lhsFn, op, 
				rhsParamName, rhsValue, rhsFieldDef, rhsFn, negate);
	}

	private Object tokenToNumber() {
		Token t = token();
		String nStr = token().str;
		int base = 10;
		if (nStr.contains("x")) {
			base = 16;
			nStr = nStr.substring(2);
		} else if (nStr.contains("b")) {
			base = 2;
			nStr = nStr.substring(2);
		}

		int len = nStr.length();
		if (nStr.equals("NaN")) {
			return Double.NaN;
		}
		if (nStr.equals("Infinity")) {
			return Double.POSITIVE_INFINITY;
		}
		
		if (t.type == T_TYPE.NUMBER_DOUBLE || t.type == T_TYPE.NUMBER_FLOAT) {
			try {
				if (t.type == T_TYPE.NUMBER_FLOAT) {
					return Float.parseFloat(nStr.substring(0, len-1));
				}
				return Double.parseDouble(nStr);
			} catch (NumberFormatException e) {
				//TODO remove this, DIgDecimal/BigInt cannot be specified in the query String! (check!)
				
				//TODO eehh, this is dirty, Exception as part of normal execution.
				//But how else can we do this? Parse manually?
				return new BigDecimal(nStr);
			}
		}
		
		if (t.type == T_TYPE.NUMBER_INT || t.type == T_TYPE.NUMBER_LONG) {
			try {
				if (t.type == T_TYPE.NUMBER_INT) {
					return Integer.parseInt(nStr, base);
				} 
				return Long.parseLong(nStr.substring(0, len-1), base);
			} catch (NumberFormatException e) {
				//TODO eehh, this is dirty, Exception as part of normal execution.
				//But how else can we do this? Parse manually?
				try {
					return new BigInteger(nStr, base);
				} catch (NumberFormatException e2) {
					throw tokenParsingError("Error parsing number '" + t.str + "'");
				}
			}
		} 
		throw new IllegalArgumentException(token().type.name());
	}

	private FNCT_OP parseMethodName(QueryFunction baseObjectFn) {
		if (baseObjectFn.op() == FNCT_OP.THIS) {
			if (match(T_TYPE.MATH)) {
				tInc();
				assertAndInc(T_TYPE.DOT);
				switch (token().str) {
				case "abs": return FNCT_OP.Math_abs;
				case "sqrt": return FNCT_OP.Math_sqrt;
				case "cos": return FNCT_OP.Math_cos; 
				case "sin": return FNCT_OP.Math_sin;
				default:
					break;
				}
			}
		}
		
		if (!hasMoreTokens(1) || !match(1, T_TYPE.OPEN)) {
			//not a method
			return null;
		}
		
		Class<?> baseType = baseObjectFn.getReturnType();
		
		if (baseType == null) {
			//This disallows calling functions on implicit parameters.
			//But since we can't always determine the base type from the function name, we'd have
			//to do late binding, determine the method to be called after getting the type at 
			//runtime. -> bad...
			if (baseObjectFn.op() == FNCT_OP.PARAM) {
				throw tokenParsingError("Late binding not supported, please specify parameters"
						+ " with setParameters() for '" + token().str + "'");
			}
			throw tokenParsingError("Late binding not supported, cannot call methods on "
					+ "results of xyz.get(...)");
		}
		
		Token t = token();
		if (String.class == baseType) {
			switch (t.str) {
			case "contains": return FNCT_OP.STR_contains_NON_JDO;
			case "matches": return FNCT_OP.STR_matches;
			case "startsWith": return FNCT_OP.STR_startsWith;
			case "endsWith": return FNCT_OP.STR_endsWith;
			case "indexOf": return FNCT_OP.STR_indexOf1;
			case "substring": return FNCT_OP.STR_substring1;
			case "toLowerCase": return FNCT_OP.STR_toLowerCase;
			case "toUpperCase": return FNCT_OP.STR_toUpperCase;
			case "length": return FNCT_OP.STR_length;
			case "trim": return FNCT_OP.STR_trim;
			}
		}
		if (Map.class.isAssignableFrom(baseType)) {
			switch (t.str) {
			case "containsKey": return FNCT_OP.MAP_containsKey;
			case "containsValue": return FNCT_OP.MAP_containsValue;
			case "isEmpty": return FNCT_OP.MAP_isEmpty;
			case "size": return FNCT_OP.MAP_size;
			case "get": return FNCT_OP.MAP_get;
			}
		}
		if (List.class.isAssignableFrom(baseType)) {
			switch (t.str) {
			case "get": return FNCT_OP.LIST_get;
			}
		}
		if (Collection.class.isAssignableFrom(baseType)) {
			switch (t.str) {
			case "contains": return FNCT_OP.COLL_contains;
			case "isEmpty": return FNCT_OP.COLL_isEmpty;
			case "size": return FNCT_OP.COLL_size;
			}
		}
		if (Enum.class.isAssignableFrom(baseType)) {
			switch (t.str) {
			case "toString": return FNCT_OP.ENUM_toString;
			case "ordinal": return FNCT_OP.ENUM_ordinal;
			}
		}
		throw tokenParsingError("Function name \"" + t.str + "\"");
	}

	private QueryFunction parseOperator(QueryFunction lhs) {
		Token tOp = token();
		tInc();
		QueryFunction rhs = parseFunction(THIS);
		switch (tOp.type) {
		case EQ:
			Class<?> rt = lhs.getReturnType();
			if (rt == Boolean.TYPE || rt == Boolean.class) {
				return QueryFunction.createJava(FNCT_OP.EQ_BOOL, THIS, lhs, rhs);
			} else if (Number.class.isAssignableFrom(rt)) {
				return QueryFunction.createJava(FNCT_OP.EQ_NUM, THIS, lhs, rhs);
			}
			return QueryFunction.createJava(FNCT_OP.EQ_OBJ, THIS, lhs, rhs);
		case G:
		case GE:
		case L:
		case LE:
		case PLUS:
		case MINUS:
		case MUL:
		case DIV:
		default: throw new UnsupportedOperationException("Not supported: " + tOp.str + 
				" near position " + tOp.pos);
		}
	}
	
	private QueryFunction parseFunction(QueryFunction baseObjectFn) {
		//check for (additional/unnecessary) parenthesis 
		if (match(T_TYPE.OPEN)) {
			tInc();
			QueryFunction f = parseFunction(baseObjectFn);
			if (!match(T_TYPE.CLOSE)) {
				//for example for the second term in "myBool == (1==1)"
				f = parseOperator(f);
			}
			assertAndInc(T_TYPE.CLOSE);
			return f;
		}
		
		
		String name = token().str;

		//first check for 'string' because the String may match a fieldName
		if (match(T_TYPE.STRING)) {
			Object constant = token().str;
			tInc();
			QueryFunction cf = QueryFunction.createConstant(constant);
			return tryParsingChainedFunctions(cf);
		}
		
		
		ZooFieldDef fieldDef = null;
		if (baseObjectFn.isPC()) {
			ZooClassDef contextType = baseObjectFn.getReturnTypeClassDef();
			if (contextType == clsDef) {
				fieldDef = fields.get(name);
			} else { 
				ZooFieldDef[] fields = contextType.getAllFields();
				for (int i = 0; i < fields.length; i++) {
					if (fields[i].getName().equals(name)) {
						fieldDef = fields[i];
						break;
					}
				}
			}
		}
		
		//we have to check here because constants come with the THIS-type, i.e. a context type
		if (fieldDef != null) {
			Field field = null;
			try {
				field = fieldDef.getJavaField();
				if (field == null) {
					throw tokenParsingError(
							"Field name not found '" + name + "' in " + clsDef.getClassName());
				}
			} catch (SecurityException e) {
				throw DBLogger.newUser("Field not accessible: " + name, e);
			}
			QueryFunction ret;
			if (fieldDef.isPersistentType()) {
				ret = QueryFunction.createFieldRef(baseObjectFn, fieldDef);
			} else {
				ret = QueryFunction.createFieldSCO(baseObjectFn, fieldDef);
			}
			tInc();
			return tryParsingChainedFunctions(ret);
		} else if (match(T_TYPE.THIS)) {
			tInc();
			if (hasMoreTokens() && match(T_TYPE.DOT)) {
				tInc();
				//ignore 'this.'
				return parseFunction(baseObjectFn);
			} else {
				return THIS;
			}
		}
		
		//TODO use switch() ?!?!
		if (match(T_TYPE.NULL)) {
			tInc();
			return QueryFunction.createConstant(QueryTerm.NULL);
		} else if (match(T_TYPE.NUMBER_INT) || match(T_TYPE.NUMBER_LONG) || 
				match(T_TYPE.NUMBER_FLOAT) || match(T_TYPE.NUMBER_DOUBLE)) {
			Object constant = tokenToNumber();
			tInc();
			return QueryFunction.createConstant(constant);
		} else if (match(T_TYPE.TRUE)) {
			tInc();
			return QueryFunction.createConstant(Boolean.TRUE);
		} else if (match(T_TYPE.FALSE)) {
			tInc();
			return QueryFunction.createConstant(Boolean.FALSE);
		} else if(match(T_TYPE.COLON)) {
			tInc();
			String paramName = token().str;
			tInc();
			QueryParameter p = addImplicitParameter(null, paramName);
			QueryFunction pF = QueryFunction.createParam(p);
			return tryParsingChainedFunctions(pF);
		}
		
		FNCT_OP fnType = parseMethodName(baseObjectFn);
		if (fnType == null) {
			//okay, not a field, let's assume this is a parameter... 
			QueryParameter p = getParameter(name);
			tInc();
			QueryFunction pF = QueryFunction.createParam(p);
			if (hasMoreTokens() && match(T_TYPE.DOT)) {
				tInc();
				return parseFunction(pF);
			} else {
				return pF;
			}
		}
		tInc();

		assertAndInc(T_TYPE.OPEN);
		QueryFunction[] args = new QueryFunction[fnType.argCount()+1];
		args[0] = baseObjectFn;
		QueryFunction localThis = THIS; 
		int pos = 0;
		do {
			for (; pos < fnType.args().length; pos++) {
				if (match(T_TYPE.CLOSE)) {
					throw tokenParsingError("Expected type '" + fnType.args()[pos] + "', but got "
							+ "nothing");
				}
				args[pos+1] = parseFunction(localThis);
				//check and implement recursive parsing of functions!
				args[pos+1] = tryParsingChainedFunctions(args[pos+1]);
				if (pos+1 < fnType.args().length) {
					assertAndInc(T_TYPE.COMMA);
				}
				Class<?> pType = fnType.args()[pos];
				Class<?> rType = args[pos+1].getReturnType(); 
				TypeConverterTools.checkAssignability(pType, rType);
			}
			if (match(T_TYPE.COMMA)) {
				//try different method signature
				fnType = fnType.biggerAlternative();
				if (fnType == null) {
					throw tokenParsingError("Expected ')' but got '" + token().msg() + "'");
				}
				args = Arrays.copyOf(args, fnType.argCount()+1);
				tInc();
				continue;
			} else {
				break;
			}
		} while (true);
		QueryFunction retFn = QueryFunction.createJava(fnType, args);
		assertAndInc(T_TYPE.CLOSE);
		//if this is a chained function, we keep parsing
		return tryParsingChainedFunctions(retFn);
	}
	
	private void assertAndInc(T_TYPE type) {
		if (!match(type)) {
			if (token().type == T_TYPE.OPEN) {
				throw tokenParsingError("Is the function name spelled correctly? Expected '" + 
						type + "' but got '" + token().msg() + "'");
			}
			throw tokenParsingError("Expected '" + type + "' but got '" + token().msg());
		}
		tInc();
	}
	
	private QueryFunction tryParsingChainedFunctions(QueryFunction qf) {
		while (hasMoreTokens() && match(T_TYPE.DOT)) {
			tInc();
			qf = parseFunction(qf);
		}
		return qf;
	}
	
	private void parseParameters() {
		while (hasMoreTokens()) {
			String typeName = token().str;
			tInc();
			
			String paramName = token().str;
			tInc();
			updateParameterType(typeName, paramName);
			if (!hasMoreTokens() || !match(T_TYPE.COMMA)) {
				return;
			}
			tInc(); // COMMA
		}
	}
	
	private QueryParameter addImplicitParameter(Class<?> type, String name) {
		for (int i = 0; i < parameters.size(); i++) {
			if (parameters.get(i).getName().equals(name)) {
				throw tokenParsingError("Duplicate parameter name: '" + name + "'");
			}
		}
		QueryParameter param = new QueryParameter(type, name, DECLARATION.IMPLICIT);
		this.parameters.add(param);
		return param;
	}
	
	private QueryParameter getParameter(String name) {
		for (int i = 0; i < parameters.size(); i++) {
			if (parameters.get(i).getName().equals(name)) {
				return parameters.get(i);
			}
		}
		//this can happen if parameters are declared with the PARAMTERS keyword 
		QueryParameter param = new QueryParameter(null, name, DECLARATION.UNDECLARED);
		this.parameters.add(param);
		return param;
	}
	
	private void updateParameterType(String typeName, String name) {
		for (int i = 0; i < parameters.size(); i++) {
			QueryParameter p = parameters.get(i);
			if (p.getName().equals(name)) {
				if (p.getDeclaration() != DECLARATION.UNDECLARED) {
					throw tokenParsingError("Duplicate parameter name: '" + name + "'");
				}
				Class<?> type = QueryParser.locateClassFromShortName(typeName);
				p.setType(type);
				if (ZooPC.class.isAssignableFrom(type)) {
					//TODO we should have a local session field here...
					ZooClassDef typeDef = clsDef.getProvidedContext().getSession(
							).getSchemaManager().locateSchema(typeName).getSchemaDef();
					p.setTypeDef(typeDef);
				}
				p.setDeclaration(DECLARATION.PARAMETERS);
				return;
			}
		}
		throw tokenParsingError("Parameter not used in query: '" + name + "'");
	}

	
	private void parseRange() {
		Token t = token();
		if (match(T_TYPE.NUMBER_INT) || match(T_TYPE.NUMBER_LONG)) {
			rangeMin = Long.parseLong(t.str);
			if (rangeMin < 0) {
				throw tokenParsingError("RANGE minimum must be >= 0, but got: " + rangeMin);
			}
		} else {
			//parameter
			boolean isImplicit = match(T_TYPE.COLON);
			if (isImplicit) {
				tInc();
				String minParamName = token().str;
				rangeMinParam = addImplicitParameter(Long.TYPE, minParamName);
			} else {
				throw tokenParsingError("RANGE can only contain numbers or implicit parameters, "
						+ "but found '" + t.str + "'");
			}
		}
		tInc();
		
		assertAndInc(T_TYPE.COMMA);
		
		t = token();
		if (match(T_TYPE.NUMBER_INT) || match(T_TYPE.NUMBER_LONG)) {
			rangeMax = Long.parseLong(t.str);
			if (rangeMax < 0) {
				throw tokenParsingError("RANGE maximum must be >= 0, but got: " + rangeMax);
			}
			if (rangeMinParam == null && rangeMax < rangeMin) {
				throw tokenParsingError("RANGE maximum must be >= minimum, but got: " + rangeMax);
			}
		} else {
			//parameter
			boolean isImplicit = match(T_TYPE.COLON);
			if (isImplicit) {
				tInc();
				String maxParamName = token().str;
				rangeMaxParam = addImplicitParameter(Long.TYPE, maxParamName);
			} else {
				throw tokenParsingError("RANGE can only contain numbers or implicit parameters, "
						+ "but found '" + t.str + "'");
			}
		}
		tInc();
	}
	
	private void parseOrdering() {
		order.clear();
		
		while (true) {
			String attrName = token().str;
			ZooFieldDef f = fields.get(attrName);
			if (f == null) {
				throw tokenParsingError("Field '" + attrName + "' not found");
			}
			if (!f.isPrimitiveType() && !f.isString()) {
				throw tokenParsingError("Field not sortable: " + f);
			}
			for (Pair<ZooFieldDef, Boolean> p2: order) {
				if (p2.getA().equals(f)) {
					throw tokenParsingError("Field '" + f + "' is sorted twice");
				}
			}
			tInc();
			
			if (match(T_TYPE.ASC) || match(T_TYPE.ASCENDING)) {
				order.add(new Pair<ZooFieldDef, Boolean>(f, true));
			} else if (match(T_TYPE.DESC) || match(T_TYPE.DESCENDING)) {
				order.add(new Pair<ZooFieldDef, Boolean>(f, false));
			} else {
				throw tokenParsingError("Please check syntax for ordering");
			}
			tInc();

			if (!hasMoreTokens() || !match(T_TYPE.COMMA)) {
				return;
			}
			tInc(); //comma
		}
	}
	
	public static void parseOrdering(final String input, int pos, 
			List<Pair<ZooFieldDef, Boolean>> ordering, ZooClassDef candClsDef) {
		ordering.clear();
		if (input == null) {
			return;
		}
		QueryParserV3 p2 = new QueryParserV3(input, candClsDef, null, ordering, -1, -1);
		p2.str = input;
		p2.tokens = p2.tokenize(input);
		if (p2.tokens.isEmpty()) {
			return;
		}
		p2.parseOrdering();
		if (p2.hasMoreTokens()) {
			throw p2.tokenParsingError("Unexpected characters '" + p2.token().msg() + "'");
		}
		ordering.addAll(p2.order);
	}
	
	
	private static enum T_TYPE {
		L_AND("&&"), L_OR("||"), L_NOT("!"),
		B_AND("&"), B_OR("|"), B_NOT("~"),
		EQ("=="), NE("!="), LE("<="), GE(">="), L("<"), G(">"),
		PLUS("+"), MINUS("-"), MUL("*"), DIV("/"), MOD("%"),
		OPEN("("), CLOSE(")"), // (, )
		COMMA(","), DOT("."), COLON(":"), //QUOTE, DQUOTE,
		AVG(true), SUM(true), MIN(true), MAX(true), COUNT(true),
		//PARAM, PARAM_IMPLICIT,
		SELECT(true), UNIQUE(true), INTO(true), FROM(true), WHERE(true), 
		ASC(true), DESC(true), ASCENDING(true), DESCENDING(true), TO(true), 
		PARAMETERS(true), VARIABLES(true), IMPORTS(true), GROUP(true), 
		ORDER(true), BY(true), RANGE(true),
		//TODO why don't these have strings as constructor arguments?
		J_STR_STARTS_WITH, J_STR_ENDSWITH,
		J_COL_CONTAINS,
		MATH("Math"),
		THIS("this"), F_NAME, STRING, TRUE("true"), FALSE("false"), NULL("null"),
		NUMBER_INT, NUMBER_LONG, NUMBER_FLOAT, NUMBER_DOUBLE, 
		//MAP
		CONTAINSKEY, CONTAINSVALUE, 
		LETTERS; //fieldName, paramName, JDOQL keyword
		
		//keywords can be all lower case or all upper case, see spec B 14.
		private final boolean isKeyword;
		private final String str;
		
		T_TYPE(String str) {
			this.str = str;
			this.isKeyword = false;
		}
		T_TYPE() {
			this.str = this.name();
			this.isKeyword = false;
		}
		T_TYPE(boolean isKeyword) {
			this.str = this.name();
			this.isKeyword = isKeyword;
		}
		public boolean matches(Token token) {
			if (str.equals(token.str)) {
				return true;
			}
			if (isKeyword && str.toLowerCase().equals(token.str)) {
				return true;
			}
			return false;
		}
		@Override
		public String toString() {
			return str;
		}
		public boolean isKeyword() {
			return isKeyword;
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
			this(type, type.toString(), pos);
		}
		@Override
		public String toString() {
			return type.name() + " - \"" + str + "\" -- " + pos;
		}
		public String msg() {
			return str == null ? type.name() : str;
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
		
		if (!isFinished(1)) {
			char c2 = charAt(1);
			String tok = "" + c + c2;
			switch (tok) {
			case "&&": t = new Token(T_TYPE.L_AND, pos); break;  
			case "||": t = new Token(T_TYPE.L_OR, pos); break;
			case "==" : t = new Token(T_TYPE.EQ, pos); break;
			case "!=": t = new Token(T_TYPE.NE, pos); break;
			case "<=" : t = new Token(T_TYPE.LE, pos); break;
			case ">=": t = new Token(T_TYPE.GE, pos); break;
			}
			if (t != null) {
				inc(2);
				return t;
			}
		}

		//separate check to avoid collision with 2-chars
		switch (c) {
		case '&': t = new Token(T_TYPE.B_AND, pos); break;
		case '|': t = new Token(T_TYPE.B_OR, pos); break;
		case '!': t = new Token(T_TYPE.L_NOT, pos); break;
		case '>': t = new Token(T_TYPE.G, pos); break;
		case '<': t = new Token(T_TYPE.L, pos); break;
		}
		if (t != null) {
			inc();
			return t;
		}

		if (isFinished()) {
			throw DBLogger.newUser("Error parsing query at pos " + pos + ": " + str);
		}
		
		if (c >= '0' && c <= '9') {
			return parseNumber();
		}
		
		switch (c) {
		case '-':
			inc();
			c = charAt0();
			if (c >= '0' && c <= '9') {
				inc(-1);
				return parseNumber();
			}
			return new Token(T_TYPE.MINUS, pos);
		case '"':
		case '\'':
			return parseString();
		case ':':
			return parseFieldOrParam();
		case 't':
		case 'T':
		case 'f':
		case 'F':
			t = parseBoolean();
			if (t != null) {
				return t;
			}
			break;
		case 'n':
		case 'N':
			t = parseNull();
			if (t != null) {
				return t;
			}
			break;
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
		T_TYPE type = T_TYPE.NUMBER_INT;
		
		while (!isFinished()) {
			c = charAt0();
			if ((c >= '0' && c <= '9') || c=='x' || c=='b' || c=='.' || 
					c=='L' || c=='l' || c=='F' || c=='f' || 
					(base==16 && ((c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))) {
				switch (c) {
				case 'x' : base = 16; break;
				case 'b' : base = 2; break;
				case '.' : type = T_TYPE.NUMBER_DOUBLE; break;
				}
				v += c;
				inc();
				continue;
			}
			break;
		}
		char last = v.charAt(v.length()-1); 
		if (last > '9') {
			if (base == 10 && (last == 'f' || last == 'F')) {
				type = T_TYPE.NUMBER_FLOAT;
			} else if (last == 'l' || last == 'L') {
				type = T_TYPE.NUMBER_LONG;
			}
		}
		return new Token(type, v, pos0);
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
		if (paramName.length() == 0) {
			throw DBLogger.newUser("Cannot parse query at position " + pos + ": " + c);
		}
		return new Token(T_TYPE.LETTERS, paramName, pos0);
	}

	private Token parseBoolean() {
		int pos0 = pos();
		if (!isFinished(3) && substring(pos0, pos0+4).equals("true") 
				&& (isFinished(4) || isWS(charAt(4)) || charAt(4)==')')) {
			inc(4);
			return new Token(T_TYPE.TRUE, pos0);
		} else if (!isFinished(3) && substring(pos0, pos0+4).equals("this") 
				&& (isFinished(4) || isWS(charAt(4)) || charAt(4)==')')) {
			inc(4);
			return new Token(T_TYPE.THIS, pos0);
		} else if (!isFinished(4) && substring(pos0, pos0+5).equals("false") 
				&& (isFinished(5) || isWS(charAt(5)) || charAt(5)==')')) {
			inc(5);
			return new Token(T_TYPE.FALSE, pos0);
		}
		return null;  //not a boolean...
	}
	
	private Token parseNull() {
		int pos0 = pos();
		if (substring(pos0, pos0+4).equals("null") 
				&& (isFinished(4) || isWS(charAt(4)) || charAt(4)==')')) {
			inc(4);
			return new Token(T_TYPE.NULL, pos0);
		}
		return null;  //not a null...
	}
	
	private void skipWS() {
		while (!isFinished() && isWS(charAt0())) {
			inc();
		}
		return;
	}

	public long getRangeMin() {
		return rangeMin;
	}

	public long getRangeMax() {
		return rangeMax;
	}

	public QueryParameter getRangeMinParam() {
		return rangeMinParam;
	}

	public QueryParameter getRangeMaxParam() {
		return rangeMaxParam;
	}
}