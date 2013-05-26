/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.tools.internal;

import java.util.IdentityHashMap;

import javax.jdo.ObjectState;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.util.PrimLongMapLI;

public class ObjectCache {

	private IdentityHashMap<Class<?>, ZooClassDef> sMapC = 
			new IdentityHashMap<Class<?>, ZooClassDef>();
	
	private PrimLongMapLI<ZooClassDef> sMapI = new PrimLongMapLI<ZooClassDef>();
	
	private PrimLongMapLI<ZooPCImpl> oMap = new PrimLongMapLI<ZooPCImpl>(); 
	
	public ZooClassDef getSchema(long clsOid) {
		return sMapI.get(clsOid);
	}

	public ZooClassDef getSchema(Class<?> cls) {
		return sMapC.get(cls);
	}

	public ZooPCImpl findCoByOID(long oid) {
		return oMap.get(oid);
	}

	public void addToCache(ZooPCImpl obj, ZooClassDef classDef, long oid,
			ObjectState hollowPersistentNontransactional) {
		oMap.put(oid, obj);
	}

	public void addSchema(long sOid, ZooClassDef schemaDef) {
		sMapI.put(sOid, schemaDef);
		sMapC.put(schemaDef.getJavaClass(), schemaDef);
	}

}
