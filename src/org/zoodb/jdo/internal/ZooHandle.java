package org.zoodb.jdo.internal;

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
		return (byte) node.readAttr(oid, schema.getSchemaDef(), getAttrHandle(attrName));
	}

	public boolean getAttrBool(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	public short getAttrShort(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	public int getAttrInt(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	public long getAttrLong(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	public char getAttrChar(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	public float getAttrFloat(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	public double getAttrDouble(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	public String getAttrString(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	public ZooHandle getAttrRefHandle(String attrName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

}
