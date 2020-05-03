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

import org.zoodb.internal.ZooClassDef;


/**
 * Query parameter instances are used to assign values to parameters in query after compilation.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryVariable {
	
	public interface Consumer {
		void setValue(QueryVariable param, Object value);
	}
	
	public enum VarDeclaration {
		/** root variable */
		ROOT,
		/** implicit with REF */
		IMPLICIT,
		/** in query with VARIABLES */
		VARIABLES,
		/** via API with setVariable */
		API
	}
	
	private Class<?> type;
	private final String name;
	private VarDeclaration declaration;
	private ZooClassDef typeDef;
	private final int id;

	public QueryVariable(Class<?> type, String name, VarDeclaration declaration, int id) {
		this.type = type;
		this.name = name;
		this.declaration = declaration;
		this.id = id;
	}
	
	public Object getName() {
		return name;
	}
	
	public Class<?> getType() {
		return type;
	}
	
	public void setType(Class<?> type) {
		this.type = type;
	}

	public VarDeclaration getDeclaration() {
		return declaration;
	}

	public void setDeclaration(VarDeclaration declaration) {
		this.declaration = declaration;		
	}

	public ZooClassDef getTypeDef() {
		return typeDef;
	}
	
	public void setTypeDef(ZooClassDef typeDef) {
		this.typeDef = typeDef;
	}

	@Override
	public String toString() {
		return (type == null ? "?" : type.getName())  + " " + name;
	}

	public int getId() {
		return id;
	}
}