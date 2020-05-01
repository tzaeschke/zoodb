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
package org.zoodb.internal;

import java.util.Date;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Util;
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
	private ZooPC pcObj;
	private boolean isInvalid = false;
	
    public ZooHandleImpl(GenericObject go, ZooClassProxy versionProxy) {
        this(go.getOid(), versionProxy, null, go);
    }

    private ZooHandleImpl(long oid, ZooClassProxy versionProxy, ZooPC pc, GenericObject go) {
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
		checkWrite();
		if (gObj != null) {
			getGenericObject().jdoZooMarkDeleted();
		}
		if (pcObj != null) {
			session.deletePersistent(pcObj);
		}
	}
	
	private void checkRead() {
		check(false);
	}

	private void checkWrite() {
		check(true);
	}

	private void check(boolean write) {
		if (isInvalid) {
			throw new IllegalStateException("This object has been invalidated/deleted.");
		}
		versionProxy.checkInvalid(write);
		if (session.isClosed()) {
			throw new IllegalStateException("Session is closed.");
		}
		if (gObj != null && gObj.jdoZooIsDeleted()) {
			throw new IllegalStateException("Object is deleted.");
		}
	}

	@Override
	public ZooClass getType() {
		checkRead();
		return versionProxy;
	}

	@Override
	public Object getJavaObject() {
		checkRead();
		if (pcObj == null) {
			if (gObj != null && (gObj.jdoZooIsNew() || gObj.jdoZooIsDirty())) {
        		//TODO  the problem here is the initialisation of the PC, which would require
        		//a way to serialize GOs into memory and deserialize them into an PC.
				throw new UnsupportedOperationException("Cannot convert new or dirty handles " +
						"into Java objects. Please commit() first or create Java object directly.");
			}
			pcObj = (ZooPC) session.getObjectById(oid);
		}
		return pcObj;
	}

	@Override
	public Object getValue(String attrName) {
		checkRead();
		return versionProxy.getField(attrName).getValue(this);
	}

	@Override
	public void setValue(String attrName, Object val) {
		findField(attrName).setValue(this, val);
	}

	private ZooField findField(String attrName) {
		checkRead();
		ZooField f = versionProxy.getField(attrName);
		if (f == null) {
			throw DBLogger.newUser("Field not found: " + attrName);
		}
		return f;
	}

	ZooPC internalGetPCI() {
		return pcObj;
	}
	
	@Override
	public String toString() {
		return Util.oidToString(oid);
	}

	public void invalidate() {
		isInvalid = true;
	}

}
