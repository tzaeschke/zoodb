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

import java.util.ArrayList;
import java.util.EnumSet;

import org.zoodb.internal.query.QueryParserV4.T_TYPE;
import org.zoodb.internal.util.DBLogger;

public class QueryTokenizer {

	private int pos = 0;
	private final String str;
	private boolean hasPostWhereKeywords = false;

	static class Token {
		final T_TYPE type;
		final String str;
		final int pos;
		public Token(T_TYPE type, String str, int pos) {
			this.type = type;
			this.str = str;
			this.pos = pos;
		}
		public Token(T_TYPE type, String str, int pos, EnumSet<T_TYPE> types) {
			this(type, str, pos);
			types.add(type);
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
	
	QueryTokenizer(String query) {
		str = query;
	}
	
	
	ArrayList<Token> tokenize() {
		try {
			ArrayList<Token> r = new ArrayList<>();
			pos = 0;
			while (pos() < str.length()) {
				skipWS();
				if (isFinished()) {
					break;
				}
				r.add( readToken() );
			}
			return r;
		} catch (StringIndexOutOfBoundsException e) {
			throw DBLogger.newUser("Parsing error: unexpected end at position " + pos + 
					". Query= " + str);
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
		Token t = value.length() == 1 ?
				new Token(T_TYPE.CHAR, value, pos0) : 
				new Token(T_TYPE.STRING, value, pos0);
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
		//keywords
		switch (paramName) {
		case "ASC":
		case "asc":
		case "ASCENDING":
		case "ascending":
			return new Token(T_TYPE.ASCENDING, paramName, pos0);
		case "AVG":
		case "avg":
			return new Token(T_TYPE.AVG, paramName, pos0);
		case "BY":
		case "by":
			return new Token(T_TYPE.BY, paramName, pos0);
		case "DESC":
		case "desc":
		case "DESCENDING":
		case "descending":
			return new Token(T_TYPE.DESCENDING, paramName, pos0);
		case "FROM":
		case "from":
			return new Token(T_TYPE.FROM, paramName, pos0);
		case "GROUP":
		case "group":
			hasPostWhereKeywords = true; 
			return new Token(T_TYPE.GROUP, paramName, pos0);
		case "IMPORTS":
		case "imports":
			hasPostWhereKeywords = true; 
			return new Token(T_TYPE.IMPORTS, paramName, pos0);
		case "INTO":
		case "into":
			return new Token(T_TYPE.INTO, paramName, pos0);
		case "Math":
			return new Token(T_TYPE.MATH, paramName, pos0); 
		case "MAX":
		case "max":
			return new Token(T_TYPE.MAX, paramName, pos0);
		case "MIN":
		case "min":
			return new Token(T_TYPE.MIN, paramName, pos0);
		case "ORDER":
		case "order":
			hasPostWhereKeywords = true; 
			return new Token(T_TYPE.ORDER, paramName, pos0);
		case "PARAMETERS":
		case "parameters":
			hasPostWhereKeywords = true; 
			return new Token(T_TYPE.PARAMETERS, paramName, pos0);
		case "RANGE":
		case "range":
			hasPostWhereKeywords = true; 
			return new Token(T_TYPE.RANGE, paramName, pos0);
		case "SELECT":
		case "select":
			return new Token(T_TYPE.SELECT, paramName, pos0);
		case "this":
			return new Token(T_TYPE.THIS, paramName, pos0);
		case "UNIQUE":
		case "unique":
			return new Token(T_TYPE.UNIQUE, paramName, pos0);
		case "VARIABLES":
		case "variables":
			hasPostWhereKeywords = true; 
			return new Token(T_TYPE.VARIABLES, paramName, pos0);
		case "WHERE":
		case "where":
			return new Token(T_TYPE.WHERE, paramName, pos0);
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


	public boolean hasPostWhereKeywords() {
		return hasPostWhereKeywords;
	}

}
