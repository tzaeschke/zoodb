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
import java.util.List;

import org.zoodb.internal.ZooClassDef;


/**
 * Query parameter instances are used to assign values to parameters in query after compilation.
 * 
 * @author Tilmann Zaeschke
 */
public final class ParameterDeclaration {
	
	public enum DECLARATION {
		/** implicit with : */
		IMPLICIT,
		/** in query with PARAMETERS */
		PARAMETERS,
		/** not yet declared */
		@Deprecated
		UNDECLARED,
		/** via API with setParameters */
		API
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

	public int getPosition() {
		return pos;
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