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
