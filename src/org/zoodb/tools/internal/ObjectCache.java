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
package org.zoodb.tools.internal;

import java.util.IdentityHashMap;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.internal.util.ClassCreator;
import org.zoodb.internal.util.PrimLongMapLI;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooClass;

public class ObjectCache {

	private final Session session;
	
	private final IdentityHashMap<Class<?>, ZooClassDef> sMapC = 
			new IdentityHashMap<Class<?>, ZooClassDef>();
	
	private final PrimLongMapLI<ZooClassDef> sMapI = new PrimLongMapLI<ZooClassDef>();
	
	private final PrimLongMapLI<GOProxy> goMap = new PrimLongMapLI<GOProxy>(); 
	
	private final PrimLongMapLI<Class<?>> goClsMap = new PrimLongMapLI<Class<?>>(); 

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
				go = ((ZooHandleImpl)hdl).getGenericObject();
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
					def.getSuperClass().getName().equals(ZooPCImpl.class.getName())) {
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
