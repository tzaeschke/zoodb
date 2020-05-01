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

import org.zoodb.internal.ZooClassDef;


/**
 * Query parameter instances are used to assign values to parameters in query after compilation.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryVariable {
	
	public static interface Consumer {
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
		API;
	}
	
	private Class<?> type;
	private final String name;
	private VarDeclaration declaration;
	private ZooClassDef typeDef;
	private int id;

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