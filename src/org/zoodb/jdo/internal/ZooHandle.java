package org.zoodb.jdo.internal;

import java.util.Date;

import org.zoodb.jdo.api.Schema;

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

	public Schema getSchemaHandle() {
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
