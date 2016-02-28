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
package org.zoodb.test.jdo;

import org.zoodb.api.impl.ZooPC;

@SuppressWarnings("unused")
public class TestJdoqlKeywordFields extends ZooPC {

	public static final String[] keywordsL = 
		{"select", "from", "where", "unique", "as", "imports", 
			"parameters", "variables", "range", "to", "group", "order", "by", 
			"math", "avg", "min", "max", "sum", "This", "substring"};
	
	public static final String[] keywordsU = 
		{"SELECT", "FROM", "WHERE", "UNIQUE", "AS", "IMPORTS", 
			"PARAMETERS", "VARIABLES", "RANGE", "TO", "GROUPS", "ORDER", "BY", 
			"MATH", "AVG", "MIN", "MAX", "SUM", "THIS", "SUBSTRING"};
	
	private String select;
	private String from;
	private String where;
	private String unique;
	private String as;
	private String imports;
	private String parameters;
	private String variables;
	private String range;
	private String to;
	private String group;
	private String order;
	private String by;
	private String math;
	private String avg;
	private String min;
	private String max;
	private String sum;
	private String This;
	private String substring;
	
	private int SELECT;
	private int FROM;
	private int WHERE;
	private int UNIQUE;
	private int AS;
	private int IMPORTS;
	private int PARAMETERS;
	private int VARIABLES;
	private int RANGE;
	private int TO;
	private int GROUPS;
	private int ORDER;
	private int BY;
	private int MATH;
	private int AVG;
	private int MIN;
	private int MAX;
	private int SUM;
	private int THIS;	
	private int SUBSTRING;
	
	private TestJdoqlKeywordFields ref;
	
	private TestJdoqlKeywordFields() {
		//for JDO
	}
	
	public TestJdoqlKeywordFields(int i, String s) {
		//nothing yet
	}

	public void setRef(TestJdoqlKeywordFields pc) {
		zooActivateWrite();
		this.ref = pc;
	}
}
