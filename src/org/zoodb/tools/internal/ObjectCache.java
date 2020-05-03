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
package org.zoodb.tools.internal;

import java.util.IdentityHashMap;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.internal.util.ClassCreator;
import org.zoodb.internal.util.PrimLongMapZ;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooClass;

public class ObjectCache {

	private final Session session;
	
	private final IdentityHashMap<Class<?>, ZooClassDef> sMapC = 
			new IdentityHashMap<Class<?>, ZooClassDef>();
	
	private final PrimLongMapZ<ZooClassDef> sMapI = new PrimLongMapZ<ZooClassDef>();
	
	private final PrimLongMapZ<GOProxy> goMap = new PrimLongMapZ<GOProxy>(); 
	
	private final PrimLongMapZ<Class<?>> goClsMap = new PrimLongMapZ<Class<?>>(); 

	public ObjectCache(Session session) {
		this.session = session;
	}
	
	public ZooClassDef getSchema(long clsOid) {
		if (!sMapI.containsKey(clsOid)) {
			throw new IllegalStateException("soid=" + clsOid);
		}
		return sMapI.get(clsOid);
	}

	public ZooClassDef getSchema(Class<?> cls) {
		return sMapC.get(cls);
	}

	public void addSchema(long sOid, ZooClassDef schemaDef) {
		sMapI.put(sOid, schemaDef);
//		if (schemaDef.getJavaClass() == null) {
//			throw new IllegalStateException();
//		}
		sMapC.put(schemaDef.getJavaClass(), schemaDef);
		addGoClass(schemaDef.getVersionProxy());
	}

	/**
	 * The only reason why this class exists is that I was to lazy to create a dynamic sub-class
	 * of GenericObject, which would have required generating a constructor-method that passes
	 * the ZooClassDef argument to the ZooClassDef constructor of GenericObject. 
	 */ 
	public static class GOProxy {
		public GenericObject go;

		public GenericObject getGenericObject() {
			return go;
		}
	}
	
	public GOProxy findOrCreateGo(long oid, ZooClass def) {
		GOProxy gop = goMap.get(oid);
		if (gop == null) {
			
			GenericObject go;
			if (session.isOidUsed(oid)) {
				ZooHandleImpl hdl = session.getHandle(oid);
				go = hdl.getGenericObject();
			} else {
				go = ((ZooHandleImpl) def.newInstance(oid)).getGenericObject();
			}
			Class<?> goCls = addGoClass((ZooClassProxy)def);
			try {
				gop = (GOProxy) goCls.newInstance();
				gop.go = go;
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} 
			goMap.put(oid, gop);
		}
		return gop;
	}

	private Class<?> addGoClass(ZooClassProxy def) {
		long sOid = def.getSchemaDef().getOid();
		Class<?> goCls = goClsMap.get(sOid);
		if (goCls == null) {
			Class<?> sup;
			if (def.getSuperClass().getName().equals(PersistenceCapableImpl.class.getName()) || 
					def.getSuperClass().getName().equals(ZooPC.class.getName())) {
				sup = GOProxy.class;
			} else {
				sup = addGoClass((ZooClassProxy) def.getSuperClass());
			}
			goCls = ClassCreator.createClass(def.getName(), sup.getName());
			goClsMap.put(sOid, goCls);
			sMapC.put(goCls, def.getSchemaDef());
		}
		return goCls;
	}
	
	public Class<?> getGopClass(long soid) {
		return goClsMap.get(soid);
	}

	public ZooClassDef getClass(long soid) {
		return sMapI.get(soid);
	}

	public GOProxy findOrCreateGo(long oid, Class<?> cls) {
		if (!GOProxy.class.isAssignableFrom(cls)) {
			throw new IllegalStateException("" + cls.getName());
		}
		ZooClassDef def = sMapC.get(cls);
		return findOrCreateGo(oid, def.getVersionProxy());
	}

}
