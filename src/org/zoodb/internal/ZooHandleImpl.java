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
package org.zoodb.internal;

import java.util.Date;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooField;
import org.zoodb.schema.ZooHandle;

/**
 * Handle for direct access to object instances in the database. 
 * 
 * @author Tilmann Zaschke
 */
public class ZooHandleImpl implements ZooHandle {

	private final long oid;
	private final Node node;
	private final Session session;
	private final ZooClassProxy versionProxy;
	private GenericObject gObj;
	private ZooPCImpl pcObj;
	
    public ZooHandleImpl(GenericObject go, ZooClassProxy versionProxy) {
        this(go.getOid(), versionProxy, null, go);
    }

    private ZooHandleImpl(long oid, ZooClassProxy versionProxy, ZooPCImpl pc, GenericObject go) {
		this.oid = oid;
		this.node = versionProxy.getSchemaDef().jdoZooGetNode();
		this.session = node.getSession();
		this.versionProxy = versionProxy;
		this.pcObj = pc;
		this.gObj = go;
    }
    
    @Override
	public long getOid() {
        return oid;
    }

    @Override
	public void setOid(long oid) {
        throw new UnsupportedOperationException();
    }

	public Session getSession() {
		return session;
	}

	@Override
	public byte getAttrByte(String attrName) {
		return (Byte) findField(attrName).getValue(this);
	}

	@Override
	public boolean getAttrBool(String attrName) {
		return (Boolean) findField(attrName).getValue(this);
	}

	@Override
	public short getAttrShort(String attrName) {
		return (Short) findField(attrName).getValue(this);
	}

	@Override
	public int getAttrInt(String attrName) {
		return (Integer) findField(attrName).getValue(this);
	}

	@Override
	public long getAttrLong(String attrName) {
		return (Long) findField(attrName).getValue(this);
	}

	@Override
	public char getAttrChar(String attrName) {
		return (Character) findField(attrName).getValue(this);
	}

	@Override
	public float getAttrFloat(String attrName) {
		return (Float) findField(attrName).getValue(this);
	}

	@Override
	public double getAttrDouble(String attrName) {
		return (Double) findField(attrName).getValue(this);
	}

	@Override
	public String getAttrString(String attrName) {
		return (String) findField(attrName).getValue(this);
	}

	@Override
	public Date getAttrDate(String attrName) {
		return (Date) findField(attrName).getValue(this);
	}

	@Override
	public ZooHandle getAttrRefHandle(String attrName) {
		return (ZooHandle) findField(attrName).getValue(this);
	}

	@Override
	public long getAttrRefOid(String attrName) {
		ZooFieldProxy prx = (ZooFieldProxy) findField(attrName);
		ZooFieldDef def = prx.getFieldDef();
		if (!def.isPersistentType()) {
			throw new IllegalStateException("This attribute is not a persistent type: " + attrName);
		}
		gObj.activateRead();
		Object oid = gObj.getFieldRaw(def.getFieldPos());
		if (oid == null) {
			return 0;
		}
		return (Long)oid;
	}

    public GenericObject getGenericObject() {
        if (gObj == null) {
        	gObj = session.internalGetCache().getGeneric(oid);
        	if (gObj == null) {
        		//This can also for example if the object already exists in the database. 
        		gObj = node.readGenericObject(versionProxy.getSchemaDef(), oid);
        	}
        }
        gObj.ensureLatestVersion();
        return gObj;
    }

	@Override
	public void remove() {
		check();
		if (gObj != null) {
			getGenericObject().setDeleted(true);
		}
		if (pcObj != null) {
			session.deletePersistent(pcObj);
		}
	}
	
	private void check() {
		if (session.isClosed()) {
			throw new IllegalStateException("Session is closed.");
		}
		if (gObj != null && gObj.isDeleted()) {
			throw new IllegalStateException("Object is deleted.");
		}
	}

	@Override
	public ZooClass getType() {
		check();
		return versionProxy;
	}

	@Override
	public Object getJavaObject() {
		check();
		if (pcObj == null) {
			if (gObj != null && (gObj.isNew() || gObj.isDirty())) {
        		//TODO  the problem here is the initialisation of the PC, which would require
        		//a way to serialize GOs into memory and deserialize them into an PC.
				throw new UnsupportedOperationException("Can not convert new or dirty handles " +
						"into Java objects. Please commit() first or create Java object directly.");
			}
			pcObj = (ZooPCImpl) session.getObjectById(oid);
		}
		return pcObj;
	}

	@Override
	public Object getValue(String attrName) {
		check();
		return versionProxy.getField(attrName).getValue(this);
	}

	@Override
	public void setValue(String attrName, Object val) {
		findField(attrName).setValue(this, val);
	}

	private ZooField findField(String attrName) {
		check();
		ZooField f = versionProxy.getField(attrName);
		if (f == null) {
			throw DBLogger.newUser("Field not found: " + attrName);
		}
		return f;
	}

	ZooPCImpl internalGetPCI() {
		return pcObj;
	}
	
}
