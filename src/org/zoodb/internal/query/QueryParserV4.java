/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.ParameterDeclaration.DECLARATION;
import org.zoodb.internal.query.QueryParser.FNCT_OP;
import org.zoodb.internal.query.QueryTokenizer.Token;
import org.zoodb.internal.query.QueryVariable.VarDeclaration;
import org.zoodb.internal.query.TypeConverterTools.COMPARISON_TYPE;
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
 * V4 is build completely as 'complex' parser.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryParserV4 implements QueryParserAPI {

	private final String str;
	private final ZooClassDef clsDef;
	private final Map<String, ZooFieldDef> fields;
	private final List<ParameterDeclaration> parameters;
	private boolean[] parametersUsed; 
	private final List<QueryVariable> variables;
	private boolean[] variablesUsed; 
	private final List<Pair<ZooFieldDef, Boolean>> order;
	private ArrayList<Token> tokens;
	private int tPos = 0;
	private final QueryFunction THIS;
	private final Session session;
	
	private long rangeMin;
	private long rangeMax;
	private ParameterDeclaration rangeMinParam = null;
	private ParameterDeclaration rangeMaxParam = null;
	
	public QueryParserV4(String query, ZooClassDef clsDef, List<ParameterDeclaration> parameters,
			List<QueryVariable> variables,
			List<Pair<ZooFieldDef, Boolean>> order, long rangeMin, long rangeMax,
			Session session) {
		this.str = query; 
		this.clsDef = clsDef;
		this.fields = clsDef.getAllFieldsAsMap();
		this.parameters = parameters;
		this.variables = variables;
		this.order = order;
		this.THIS = QueryFunction.createThis(clsDef);
		this.rangeMin = rangeMin;
		this.rangeMax = rangeMax;
		this.session = session;
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
		return type == t.type;
	}
	
	private boolean match(int offs, T_TYPE type) {
		Token t = token(offs);
		return type == t.type;
	}
	
	private boolean hasMoreTokens() {
		return tPos < tokens.size();
	}
	
	private boolean hasMoreTokens(int offset) {
		return tPos+offset < tokens.size();
	}
	
	@Override
	public QueryTree parseQuery() {
		QueryTokenizer tokenizer = new QueryTokenizer(str);
		tokens = tokenizer.tokenize();
		boolean hasPostWhereKeywords = tokenizer.hasPostWhereKeywords();
		
		try {
			//Parse everything before WHERE
			//Result: Whether the query contains only a filter or also other
			
			parseSELECT();

			//We are now at the beginning of the filter clause.
			//The filter-clause may be empty...
		
			//Search and parse everything _after_ WHERE
			if (hasPostWhereKeywords) {
				int marker = tPos;
				parsePostWHERE();
				tPos = marker;
			}
			
			//Finally, parse WHERE clause
			
			//Negation is used to invert negated operand.
			//We just pass it down the tree while parsing, always inverting the flag if a '!' is
			//encountered. When popping out of a function, the flag is reset to the value outside
			//the term that was parsed in a function. Actually, it is not reset, it is never modified.
			QueryFunction qn = parseTree();
			//System.out.println("Query AST: "+ qn.toString());
			return new QueryTree(qn, rangeMin, rangeMax, rangeMinParam, rangeMaxParam);
		} catch (StringIndexOutOfBoundsException e) {
			throw tokenParsingError("Unexpected end");
		}
	}
	
	private boolean parseSELECT() {
		if (token().type == T_TYPE.SELECT && 
				hasMoreTokens() && !token(1).type.isOperator()
				 && token(1).type != T_TYPE.DOT) {
			//If it were followed by an operator it would need to be
			//a field (or a mistake..)
			T_TYPE type = token().type;
			while (type != T_TYPE.WHERE) {
				switch (type) {
				case SELECT:
					//skip SELECT
					tInc();
					break;
				case UNIQUE:
					//TODO set UNIQUE!
					tInc();
					break;
				case FROM:
					tInc();
					if (!clsDef.getClassName().equals(token().str)) {
						//TODO class name
						throw DBLogger.newUser("Class mismatch: " + token().pos + " / " + clsDef);
					}
					tInc();
					break;
				default:
					throw tokenParsingError("Unexpected keyword");
				}
				type = token().type;
			}
			if (type != T_TYPE.WHERE) {
				throw tokenParsingError("Unexpected keyword");
			}
			tInc();
			return false;
		}
		return true;
	}
	
	private void parsePostWHERE() {
		int startPostWhereClauses = -1;
		do {
			T_TYPE type = token().type;
			//followed by operator? -> not a keyword...
			if (type.isKeyword() 
					&& hasMoreTokens(1)
					&& !token(1).type.isOperator() 
					&& token(1).type != T_TYPE.DOT) {
				//End of WHERE clause -> process remaining clauses
				startPostWhereClauses = tPos;
				break;
			}
			tInc();
		} while (hasMoreTokens());
		
		if (startPostWhereClauses == -1) {
			return;
		}
		
		do {
			T_TYPE type = token().type;
			switch (type) {
			case IMPORTS: 
				throw new UnsupportedOperationException("JDO feature not supported: IMPORTS");
			case GROUP:
				match(1, T_TYPE.BY);
				throw new UnsupportedOperationException("JDO feature not supported: GROUP BY");
			case ORDER:
				match(1, T_TYPE.BY);
				tInc(2);
				parseOrdering();
				break;
			case PARAMETERS: 
				tInc();
				parseParameters();
				break;
			case RANGE: 
				tInc();
				parseRange();
				break;
			case VARIABLES: 
				tInc();
				parseVariables();
				break;
			default:
				throw tokenParsingError("Unexpected keyword");
			}
		} while (hasMoreTokens());
		
		//clean up tokens
		for (int i = tokens.size() - 1; i >= startPostWhereClauses; i--) {
			tokens.remove(i);
		}
	}
	
	QueryFunction parseTree() {
		//To throw errors if parameters are unused.
		this.parametersUsed = new boolean[parameters.size()];
		this.variablesUsed = new boolean[variables.size()];
		
		QueryFunction lhsFn;
		if (!hasMoreTokens()) {
			lhsFn = QueryFunction.createConstant(Boolean.TRUE);
		} else {
			lhsFn = parseFunction(THIS, false);
			while (hasMoreTokens()) {
				lhsFn = parseOperator(lhsFn, false);
			}
			//This catches for example a query such as "this.name"
			if (lhsFn.getReturnType() != Boolean.TYPE) {
				throw tokenParsingError("Unexpected end of query, expected operator");
			}
		}

		//verify usage
		for (int i = 0; i < parametersUsed.length; i++) {
			if (!parametersUsed[i] && 
					parameters.get(i).getDeclaration() != DECLARATION.IMPLICIT) {
				throw DBLogger.newUser("Query parameter not used: " + parameters.get(i).getName()); 
			}
		}
		for (int i = 0; i < variablesUsed.length; i++) {
			if (!variablesUsed[i]) {
				throw DBLogger.newUser("Query variable not used: " + variables.get(i).getName()); 
			}
		}
		
		return lhsFn;
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

	private QueryFunction parseOperator(QueryFunction lhs, boolean negate) {
		Token tOp = token();
		if (!tOp.type.isOperator()) {
			throw tokenParsingError("Operator expected");
		}
		tInc();
		boolean negateRhs = negate && (tOp.type == T_TYPE.L_AND || tOp.type == T_TYPE.L_OR);
		QueryFunction rhs = parseFunction(THIS, negateRhs);
		while (hasMoreTokens() && token().type.getOperatorPrecedence() > tOp.type.getOperatorPrecedence()) {
			//treat current rhs as lhs for following operator
			rhs = parseOperator(rhs, negateRhs);
		}
		COMPARISON_TYPE ct = getComparisonType(lhs, rhs, tOp);
		if (negate) {
			switch (tOp.type) {
			case EQ:
			case NE:
			case G: 
			case GE:
			case L: 
			case LE:
			case L_AND:
			case L_OR:
				break;
			default:
				throw new UnsupportedOperationException("Not supported: " + tOp.str + 
					" near position " + tOp.pos);
			}
		}
		switch (tOp.type) {
		case EQ:
			if (ct == COMPARISON_TYPE.BOOLEAN) {
				return QueryFunction.createComparison(FNCT_OP.EQ_BOOL, negate, THIS, lhs, rhs);
			}
			return QueryFunction.createComparison(FNCT_OP.EQ, negate, THIS, lhs, rhs);
		case NE:
			if (ct == COMPARISON_TYPE.BOOLEAN) {
				return QueryFunction.createComparison(FNCT_OP.NE_BOOL, negate, THIS, lhs, rhs);
			}
			return QueryFunction.createComparison(FNCT_OP.NE, negate, THIS, lhs, rhs);
		case PLUS:
			switch (ct) {
			case STRING:
				return QueryFunction.createJava(FNCT_OP.PLUS_STR, THIS, lhs, rhs);
			case DOUBLE:
			case FLOAT:
				return QueryFunction.createJava(FNCT_OP.PLUS_D, THIS, lhs, rhs);
			case LONG:
			case SHORT:
			case INT:
			case BYTE:
				return QueryFunction.createJava(FNCT_OP.PLUS_L, THIS, lhs, rhs);
			default: failOp(lhs.getReturnType(), rhs.getReturnType(), tOp.type);
			}
		case MOD:
			return QueryFunction.createJava(FNCT_OP.MOD, THIS, lhs, rhs);
		case G: 
			return QueryFunction.createComparison(FNCT_OP.G, negate, THIS, lhs, rhs);
		case GE:
			return QueryFunction.createComparison(FNCT_OP.GE, negate, THIS, lhs, rhs);
		case L: 
			return QueryFunction.createComparison(FNCT_OP.L, negate, THIS, lhs, rhs);
		case LE:
			return QueryFunction.createComparison(FNCT_OP.LE, negate, THIS, lhs, rhs);
		case L_AND:
			return QueryFunction.createJava(negate ? FNCT_OP.L_OR : FNCT_OP.L_AND, THIS, lhs, rhs);
		case L_OR:
			return QueryFunction.createJava(negate ? FNCT_OP.L_AND : FNCT_OP.L_OR, THIS, lhs, rhs);
		case MINUS:
		case MUL:
		case DIV: 
		default:
			throw new UnsupportedOperationException("Not supported: " + tOp.str + 
				" near position " + tOp.pos);
		}
	}
	
	private COMPARISON_TYPE getComparisonType(QueryFunction lhs, QueryFunction rhs, Token tOp) {
		Class<?> lrt = lhs.getReturnType();
		Class<?> rrt = rhs.getReturnType();
		//The following helps to assign types to implicit parameters
		if (lrt != rrt && lrt == null && lhs.op() == FNCT_OP.PARAM) {
			//set type of param
			((ParameterDeclaration)lhs.getConstantUnsafe()).setType(rrt);
			lhs.setReturnType(rrt);
			lrt = rrt;
		} else if (lrt != rrt && rrt == null && rhs.op() == FNCT_OP.PARAM) {
			//set type of param
			((ParameterDeclaration)rhs.getConstantUnsafe()).setType(lrt);
			rhs.setReturnType(lrt);
			rrt = lrt;
		}
		
		COMPARISON_TYPE ct = TypeConverterTools.fromTypes(lrt, rrt);
		verifyOperandApplicability(tOp.type, ct, lrt, rrt);
		if (ct == COMPARISON_TYPE.STRING) {
			//Ensure that we are dealing with Sting, not with Character
			convertCharToString(lhs);
			convertCharToString(rhs);
		}
		return ct;
	}
	
	private void convertCharToString(QueryFunction fn) {
		if (!String.class.isAssignableFrom(fn.getReturnType()) && !fn.isConstant()) {
			throw tokenParsingError("Cannot compare String to Character.");
		}
	}

	private void verifyOperandApplicability(T_TYPE op, COMPARISON_TYPE ct, Class<?> lhsCt, Class<?> rhsCt) {
		if (op == T_TYPE.EQ || op == T_TYPE.NE) {
			return;
		}
		if (ct == COMPARISON_TYPE.PC || ct == COMPARISON_TYPE.SCO) {
			//works only with EQ/NE
			failOp(lhsCt, rhsCt, op);
		}
		if (ct == COMPARISON_TYPE.STRING) {
			if (op == T_TYPE.G || op == T_TYPE.GE || op == T_TYPE.L || op == T_TYPE.LE || op == T_TYPE.PLUS) {
				return;
			}
			failOp(lhsCt, rhsCt, op);
		}
		if (ct == COMPARISON_TYPE.CHAR) {
			if (op == T_TYPE.G || op == T_TYPE.GE || op == T_TYPE.L || op == T_TYPE.LE || op == T_TYPE.PLUS) {
				return;
			}
			failOp(lhsCt, rhsCt, op);
		}
	}

	private void failOp(Class<?> lhsCt, Class<?> rhsCt, T_TYPE op) {
		throw DBLogger.newUser("Illegal operator: Cannot compare " + lhsCt + " and " + rhsCt + " with " + op);
	}
	
	private QueryFunction parseFunction(QueryFunction baseObjectFn, boolean negate) {
		if (match(T_TYPE.L_NOT)) {
			tInc();
			return parseFunction(baseObjectFn, !negate);
		}
		
		//check for (additional/unnecessary) parenthesis 
		if (match(T_TYPE.OPEN)) {
			tInc();
			QueryFunction f = parseFunction(baseObjectFn, negate);
			while (!match(T_TYPE.CLOSE)) {
				//for example for the second term in "myBool == (1==1)"
				f = parseOperator(f, negate);
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
			return tryParsingChainedFunctions(cf, negate);
		}
		if (match(T_TYPE.CHAR)) {
			Object constant = token().str.charAt(0);
			tInc();
			QueryFunction cf = QueryFunction.createConstant(constant);
			return tryParsingChainedFunctions(cf, negate);
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
			Field field;
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
			return tryParsingChainedFunctions(ret, negate);
		} else if (match(T_TYPE.THIS)) {
			if (tPos > 0 && match(-1, T_TYPE.DOT)) {
				throw tokenParsingError("Unexpected 'this'.");
			}
			tInc();
			if (hasMoreTokens() && match(T_TYPE.DOT)) {
				tInc();
				//ignore 'this.'
				return parseFunction(baseObjectFn, negate);
			} else {
				return negate(THIS, negate);
			}
		}
		
		//TODO use switch() ?!?!
		if (match(T_TYPE.NULL)) {
			tInc();
			return negate(QueryFunction.createConstant(QueryTerm.NULL), negate);
		} else if (match(T_TYPE.NUMBER_INT) || match(T_TYPE.NUMBER_LONG) || 
				match(T_TYPE.NUMBER_FLOAT) || match(T_TYPE.NUMBER_DOUBLE)) {
			Object constant = tokenToNumber();
			tInc();
			return negate(QueryFunction.createConstant(constant), negate);
		} else if (match(T_TYPE.TRUE)) {
			tInc();
			return negate(QueryFunction.createConstant(Boolean.TRUE), negate);
		} else if (match(T_TYPE.FALSE)) {
			tInc();
			return negate(QueryFunction.createConstant(Boolean.FALSE), negate);
		} else if(match(T_TYPE.COLON)) {
			tInc();
			String paramName = token().str;
			tInc();
			ParameterDeclaration p = addImplicitParameter(null, paramName);
			QueryFunction pF = QueryFunction.createParam(p);
			return tryParsingChainedFunctions(pF, negate);
		}
		
		FNCT_OP fnType = parseMethodName(baseObjectFn);
		if (fnType == null) {
			//okay, not a field, let's assume this is a parameter... 
			ParameterDeclaration p = getParameter(name);
			QueryVariable v;
			QueryFunction pF;
			if (p != null) {
				tInc();
				pF = QueryFunction.createParam(p);
			} else if ((v = getVariable(name)) != null) {
				tInc();
				pF = QueryFunction.createVariable(v);
			} else {
				throw tokenParsingError("Illegal token: \"" + name + "\"");
			}
			
			if (hasMoreTokens() && match(T_TYPE.DOT)) {
				tInc();
				return parseFunction(pF, negate);
			} else {
				return negate(pF, negate);
			}
		}
		tInc();

		//Parse function parameters. 
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
				args[pos+1] = parseFunction(localThis, false);
				//check and implement recursive parsing of functions!
				//Parameters are not negated, instead we negate the function!
				args[pos+1] = tryParsingChainedFunctions(args[pos+1], false);
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
			} else {
				break;
			}
		} while (true);
		QueryFunction retFn = QueryFunction.createJava(fnType, args);
		assertAndInc(T_TYPE.CLOSE);
		//if this is a chained function, we keep parsing
		return negate(tryParsingChainedFunctions(retFn, false), negate);
	}
	
	private QueryFunction negate(QueryFunction fn, boolean negate) {
		if (negate && (fn.getReturnType() != Boolean.TYPE)) {
			throw tokenParsingError("Cannot negate '" + fn.getReturnType() + "' type': " + token().msg());
		}
		return negate ? QueryFunction.createJava(FNCT_OP.L_NOT, THIS, fn) : fn;
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
	
	private QueryFunction tryParsingChainedFunctions(QueryFunction qf, boolean negate) {
		while (hasMoreTokens() && match(T_TYPE.DOT)) {
			tInc();
			qf = parseFunction(qf, negate);
		}
		return qf;
	}
	
	private void parseParameters() {
		while (hasMoreTokens()) {
			String typeName = token().str;
			tInc();
			
			String paramName = token().str;
			tInc();
			if (getParameter(paramName) != null || getVariable(paramName) != null) {
				throw tokenParsingError("Duplicate parameter or variable name: '" + paramName + "'");
			}
			createParameter(typeName, paramName);
			if (!hasMoreTokens() || !match(T_TYPE.COMMA)) {
				return;
			}
			tInc(); // COMMA
		}
	}
	
	private ParameterDeclaration addImplicitParameter(Class<?> type, String name) {
		for (int i = 0; i < parameters.size(); i++) {
			ParameterDeclaration p = parameters.get(i);
			if (p.getName().equals(name)) {
				if (DECLARATION.IMPLICIT != p.getDeclaration()) {
					throw tokenParsingError(
							"Implicit parameter is already explicitly declared: '" + name + "'");
				}
				if (type != p.getType()) {
					TypeConverterTools.checkAssignability(p.getType(), type);
				}
				return p;
			}
		}
		ParameterDeclaration param = new ParameterDeclaration(type, name, DECLARATION.IMPLICIT,
				this.parameters.size());
		this.parameters.add(param);
		return param;
	}
	
	private ParameterDeclaration getParameter(String name) {
		for (int i = 0; i < parameters.size(); i++) {
			ParameterDeclaration p = parameters.get(i);
			if (p.getName().equals(name)) {
				if (i < parametersUsed.length) {
					//mark non-implicit parameters as 'used'
					parametersUsed[i] = true;
				}
				return p;
			}
		}
		return null;
	}
	
	private void createParameter(String typeName, String name) {
		Class<?> type = QueryParser.locateClassFromShortName(typeName);
		ParameterDeclaration p = new ParameterDeclaration(type, name, DECLARATION.PARAMETERS,
				this.parameters.size());
		this.parameters.add(p);
		if (ZooPC.class.isAssignableFrom(type)) {
			ZooClassDef typeDef = session.getSchemaManager().locateSchema(typeName).getSchemaDef();
			p.setTypeDef(typeDef);
		}
	}

	private void parseVariables() {
		while (hasMoreTokens()) {
			String typeName = token().str;
			tInc();
			
			String varName = token().str;
			tInc();
			if (getParameter(varName) != null || getVariable(varName) != null) {
				throw tokenParsingError("Duplicate parameter or variable name: '" + varName + "'");
			}
			createVariable(typeName, varName);
			if (!hasMoreTokens() || !match(T_TYPE.COMMA)) {
				return;
			}
			tInc(); // COMMA
		}
	}
	
	private QueryVariable getVariable(String name) {
		for (int i = 0; i < variables.size(); i++) {
			if (variables.get(i).getName().equals(name)) {
				//mark variables as 'used'
				variablesUsed[i] = true;
				return variables.get(i);
			}
		}
		return null;
	}

	private void createVariable(String typeName, String name) {
		Class<?> type = QueryParser.locateClassFromShortName(typeName);
		//id=0 is preserved for the 'global' variable
		int id = variables.size() + 1;
		QueryVariable variable = new QueryVariable(type, name, VarDeclaration.VARIABLES, id);
		this.variables.add(variable);
		if (ZooPC.class.isAssignableFrom(type)) {
			ZooClassDef typeDef = session.getSchemaManager().locateSchema(typeName).getSchemaDef();
			variable.setTypeDef(typeDef);
		}
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
	
	
	enum T_TYPE {
		L_AND(4, "&&", true), L_OR(3, "||", true), L_NOT(14, "!", true),
		B_AND(7, "&", false), B_XOR(6, "^", false), B_OR(5, "|", false), B_NOT(14, "~", false),
		EQ(8, "==", true), NE(8, "!=", true), LE(9, "<=", true), GE(9, ">=", true), L(9, "<", true), G(9, ">", true),
		PLUS(11, "+", false), MINUS(11, "-", false), MUL(12, "*", false), DIV(12, "/", false), MOD(12, "%", false),
		OPEN("("), CLOSE(")"), 
		COMMA(","), DOT("."), COLON(":"), 
		AVG(true), SUM(true), MIN(true), MAX(true), COUNT(true),
		//PARAM, PARAM_IMPLICIT,
		SELECT(true), UNIQUE(true), INTO(true), FROM(true), WHERE(true), 
		ASC(true), DESC(true), ASCENDING(true), DESCENDING(true), TO(true), 
		PARAMETERS(true), VARIABLES(true), IMPORTS(true), GROUP(true), 
		ORDER(true), BY(true), RANGE(true),
		MATH("Math"),
		THIS("this"), F_NAME, STRING, CHAR, TRUE("true"), FALSE("false"), NULL("null"),
		NUMBER_INT, NUMBER_LONG, NUMBER_FLOAT, NUMBER_DOUBLE, 
		LETTERS; //fieldName, paramName, JDOQL keywords
		
		private static final int DEFAULT_PRECEDENCE = 0;
		
		//keywords can be all lower case or all upper case, see spec B 14.
		private final boolean isKeyword;
		private final boolean isLogicalOperator;
		private final String str;
		private final int operatorPrecedence; 
		
		T_TYPE(String str) {
			this(DEFAULT_PRECEDENCE, str, false);
		}
		T_TYPE(int operatorPrecedence, String str, boolean isLogicalOperator) {
			this.str = str;
			this.isKeyword = false;
			this.operatorPrecedence = operatorPrecedence;
			this.isLogicalOperator = isLogicalOperator;
		}
		T_TYPE() {
			this.str = this.name();
			this.isKeyword = false;
			this.operatorPrecedence = DEFAULT_PRECEDENCE;
			this.isLogicalOperator = false;
		}
		T_TYPE(boolean isKeyword) {
			this.str = this.name();
			this.isKeyword = isKeyword;
			this.operatorPrecedence = DEFAULT_PRECEDENCE;
			this.isLogicalOperator = false;
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
		public int getOperatorPrecedence() {
			return operatorPrecedence;
		}
		@Override
		public String toString() {
			return str;
		}
		public boolean isKeyword() {
			return isKeyword;
		}
		public boolean isOperator() {
			return operatorPrecedence != DEFAULT_PRECEDENCE;
		}
		public boolean isLogicalOperator() {
			return isLogicalOperator;
		}
	}
	

	@Override
	public long getRangeMin() {
		return rangeMin;
	}

	@Override
	public long getRangeMax() {
		return rangeMax;
	}

	public ParameterDeclaration getRangeMinParam() {
		return rangeMinParam;
	}

	public ParameterDeclaration getRangeMaxParam() {
		return rangeMaxParam;
	}
	
	public List<ParameterDeclaration> getParameters() {
		return parameters;
	}
}
