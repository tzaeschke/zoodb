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
package org.zoodb.jdo.internal;

import java.util.Date;

import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooHandle;

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
	private GenericObject gObj = null;
	
	public ZooHandleImpl(long oid, ZooClassProxy versionProxy) {
		this.oid = oid;
		this.node = versionProxy.getSchemaDef().jdoZooGetNode();
		this.session = node.getSession();
		this.versionProxy = versionProxy;
	}

    public ZooHandleImpl(GenericObject go, ZooClassProxy versionProxy) {
        this(go.getOid(), versionProxy);
        this.gObj = go;
    }

    /* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getOid()
	 */
    @Override
	public Object getOid() {
        return oid;
    }

    /* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#setOid(long)
	 */
    @Override
	public void setOid(long oid) {
        throw new UnsupportedOperationException();
    }

	public Session getSession() {
		return session;
	}

	private ZooFieldDef getAttrHandle(String attrName) {
		//TODO performance
		//Instead we should return the ID of the field, and the ClassDef should use arrays to
		//allow for quick lookup.
		//-> or we return an attrHandle.
		return versionProxy.getSchemaDef().getField(attrName);
	}
	
	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrByte(java.lang.String)
	 */
	@Override
	public byte getAttrByte(String attrName) {
		//TODO
		//return (byte) versionProxy.locateField(attrName).getValue(this);
		return node.readAttrByte(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrBool(java.lang.String)
	 */
	@Override
	public boolean getAttrBool(String attrName) {
		return node.readAttrBool(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrShort(java.lang.String)
	 */
	@Override
	public short getAttrShort(String attrName) {
		return node.readAttrShort(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrInt(java.lang.String)
	 */
	@Override
	public int getAttrInt(String attrName) {
		return node.readAttrInt(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrLong(java.lang.String)
	 */
	@Override
	public long getAttrLong(String attrName) {
		return node.readAttrLong(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrChar(java.lang.String)
	 */
	@Override
	public char getAttrChar(String attrName) {
		return node.readAttrChar(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrFloat(java.lang.String)
	 */
	@Override
	public float getAttrFloat(String attrName) {
		return node.readAttrFloat(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrDouble(java.lang.String)
	 */
	@Override
	public double getAttrDouble(String attrName) {
		return node.readAttrDouble(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrString(java.lang.String)
	 */
	@Override
	public String getAttrString(String attrName) {
		return node.readAttrString(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrDate(java.lang.String)
	 */
	@Override
	public Date getAttrDate(String attrName) {
		return node.readAttrDate(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrRefHandle(java.lang.String)
	 */
	@Override
	public ZooHandle getAttrRefHandle(String attrName) {
		long oid2 = node.readAttrRefOid(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
		throw new UnsupportedOperationException();
//		ISchema schema2 = _cache.
//		return new ZooHandle(oid2, node, session, schema2);
	}

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#getAttrRefOid(java.lang.String)
	 */
	@Override
	public long getAttrRefOid(String attrName) {
		return node.readAttrRefOid(oid, versionProxy.getSchemaDef(), getAttrHandle(attrName));
	}

    public GenericObject getGenericObject() {
        //TODO ensure uniqueness!?!? I.e. that there is only one ZooHandle for each OID
        System.out.println("TODO ensure uniqueness!?!? I.e. that there is only one ZooHandle for each OID");
        if (gObj == null) {
        	gObj = new GenericObject(versionProxy.getSchemaDef(), oid); 
        }
        gObj.ensureLatestVersion();
        return gObj;
    }

	/* (non-Javadoc)
	 * @see org.zoodb.jdo.api.ZooHand#remove()
	 */
	@Override
	public void remove() {
		check();
		getGenericObject().setDeleted(true);
	}
	
	private void check() {
		if (gObj != null && gObj.isDeleted()) {
			throw new IllegalStateException("Object is deleted.");
		}
	}

	@Override
	public ZooClass getType() {
		return versionProxy;
	}

	@Override
	public Object getJavaObject() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

}
