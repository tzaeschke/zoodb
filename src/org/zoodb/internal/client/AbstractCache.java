/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.client;

import javax.jdo.ObjectState;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.ZooClassDef;

public interface AbstractCache {

	public abstract void rollback();

	public abstract void markPersistent(ZooPCImpl pc, long oid, Node node, 
			ZooClassDef clsDef);
	
	public abstract ZooPCImpl findCoByOID(long oid);

	public abstract ZooClassDef getSchema(long clsOid);

	public abstract ZooClassDef getSchema(Class<?> cls, Node node);

	public abstract void addToCache(ZooPCImpl obj,
			ZooClassDef classDef, long oid, ObjectState state);

	ZooClassDef getSchema(String clsName);

	GenericObject getGeneric(long oid);

	public abstract void addGeneric(GenericObject genericObject);

}
