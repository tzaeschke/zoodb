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
package org.zoodb.internal.client;

import javax.jdo.ObjectState;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.ZooClassDef;

public interface AbstractCache {

	public abstract void rollback();

	public abstract void markPersistent(ZooPC pc, Node node, ZooClassDef clsDef);
	
	public abstract ZooPC findCoByOID(long oid);

	public abstract ZooClassDef getSchema(long clsOid);

	public abstract ZooClassDef getSchema(Class<?> cls, Node node);

	public abstract void addToCache(ZooPC obj,
			ZooClassDef classDef, long oid, ObjectState state);

	ZooClassDef getSchema(String clsName);

	GenericObject getGeneric(long oid);

	public abstract void addGeneric(GenericObject genericObject);

}
