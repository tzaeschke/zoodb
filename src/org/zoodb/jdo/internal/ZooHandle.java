/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

public class ZooHandle {

	private final long oid;
	private final Node node;
	private final Session session;
	private final ISchema schema;
	
	public ZooHandle(long oid, Node node, Session session, ISchema schema) {
		this.oid = oid;
		this.node = node;
		this.session = session;
		this.schema = schema;
	}

	public long getOid() {
		return oid;
	}

	public Session getSession() {
		return session;
	}

	public ZooClass getSchemaHandle() {
		return schema;
	}

	private ZooFieldDef getAttrHandle(String attrName) {
		//TODO performance
		//Instead we should return the ID of the field, and the ClassDef should use arrays to
		//allow for quick lookup.
		//-> or we return an attrHandle.
		return schema.getSchemaDef().getField(attrName);
	}
	
	public byte getAttrByte(String attrName) {
		return node.readAttrByte(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public boolean getAttrBool(String attrName) {
		return node.readAttrBool(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public short getAttrShort(String attrName) {
		return node.readAttrShort(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public int getAttrInt(String attrName) {
		return node.readAttrInt(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public long getAttrLong(String attrName) {
		return node.readAttrLong(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public char getAttrChar(String attrName) {
		return node.readAttrChar(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public float getAttrFloat(String attrName) {
		return node.readAttrFloat(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public double getAttrDouble(String attrName) {
		return node.readAttrDouble(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public String getAttrString(String attrName) {
		return node.readAttrString(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public Date getAttrDate(String attrName) {
		return node.readAttrDate(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public ZooHandle getAttrRefHandle(String attrName) {
		long oid2 = node.readAttrRefOid(oid, schema.getSchemaDef(), getAttrHandle(attrName));
		throw new UnsupportedOperationException();
//		ISchema schema2 = _cache.
//		return new ZooHandle(oid2, node, session, schema2);
	}

	public long getAttrRefOid(String attrName) {
		return node.readAttrRefOid(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

}
