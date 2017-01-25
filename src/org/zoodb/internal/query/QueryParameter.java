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

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooClassDef;


/**
 * Query parameter instances are used to assign values to parameters in query after compilation.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryParameter {
	
	public static interface Consumer {
		void setValue(QueryParameter param, Object value);
	}
	
	public enum DECLARATION {
		/** implicit with : */
		IMPLICIT,
		/** in query with PARAMETERS */
		PARAMETERS,
		/** not yet declared */
		UNDECLARED,
		/** via API with setParameters */
		API;
	}
	
	private Class<?> type;
	private final String name;
	private Object value;
	//TODO this should be used at some point to execute queries on the server without loading the 
	//object
	private long oid;
	private DECLARATION declaration;
	private ZooClassDef typeDef;

	public QueryParameter(Class<?> type, String name, DECLARATION declaration) {
		this.type = type;
		this.name = name;
		this.declaration = declaration;
	}
	
	public void setValue(Object p1) {
		if (p1 != null) {
			//Check type if type is known; it is unknown for implicit parameters
			if (type != null) {
				TypeConverterTools.checkAssignability(p1, type);
			}
			value = p1;
			if (p1 instanceof ZooPC) {
				oid = ((ZooPC)p1).jdoZooGetOid();
			}
		} else {
			value = QueryTerm.NULL;
		} 
	}
	
	public Object getValue() {
		return value;
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

	public DECLARATION getDeclaration() {
		return declaration;
	}

	public void setDeclaration(DECLARATION declaration) {
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
}