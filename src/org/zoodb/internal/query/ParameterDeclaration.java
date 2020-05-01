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

import java.util.ArrayList;
import java.util.List;

import org.zoodb.internal.ZooClassDef;


/**
 * Query parameter instances are used to assign values to parameters in query after compilation.
 * 
 * @author Tilmann Zaeschke
 */
public final class ParameterDeclaration {
	
	public static interface Consumer {
		void setValue(ParameterDeclaration param, Object value);
	}
	
	public enum DECLARATION {
		/** implicit with : */
		IMPLICIT,
		/** in query with PARAMETERS */
		PARAMETERS,
		/** not yet declared */
		@Deprecated
		UNDECLARED,
		/** via API with setParameters */
		API;
	}
	
	private Class<?> type;
	private final String name;
	private DECLARATION declaration;
	private ZooClassDef typeDef;
	private final int pos;

	public ParameterDeclaration(Class<?> type, String name, DECLARATION declaration, int pos) {
		this.type = type;
		this.name = name;
		this.declaration = declaration;
		this.pos = pos;
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

	public Object getValue(Object[] params) {
		return params[pos];
	}

	public void setValue(Object[] params, Object object) {
		params[pos] = object;
	}
	
	public static void adjustValues(List<ParameterDeclaration> decls, Object[] params) {
		for (int i = 0; i < params.length; i++) {
			Object p1 = params[i];
			if (p1 != null) {
				//Check type if type is known; it is unknown for implicit parameters
				Class<?> type = decls.get(i).type;
				if (type != null) {
					TypeConverterTools.checkAssignability(p1, type);
				}
				params[i] = p1;
				//TODO??
				//TODO??
				//TODO??
				//TODO??
//				if (p1 instanceof ZooPC) {
//					oid = ((ZooPC)p1).jdoZooGetOid();
//				}
			} else {
				params[i] = QueryTerm.NULL;
			} 
		}
	}

	
	@Override
	public String toString() {
		return (type == null ? "?" : type.getName())  + " " + name;
	}

	/**
	 * 
	 * @param parameters parameter declarations
	 * @return dummy parameters
	 * @deprecated We only need this for V3. Remove this once V3 is removed
	 */
	@Deprecated
	public static Object[] createDummyParameters(ArrayList<ParameterDeclaration> parameters) {
		Object[] params = new Object[parameters.size()];
		for (int i = 0; i < parameters.size(); i++) {
			ParameterDeclaration p = parameters.get(i);
			if (p.type == String.class) {
				params[i] = "";
			} else if (Number.class.isAssignableFrom(p.type)) {
				params[i] = Integer.valueOf(0);
			} else if (p.typeDef != null) {
				params[i] = Long.valueOf(0);
			} else {
				params[i] = QueryTerm.NULL;
			}
		}
		return params;
	}
}