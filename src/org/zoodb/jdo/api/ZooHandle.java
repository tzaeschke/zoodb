package org.zoodb.jdo.api;

import java.util.Date;

public interface ZooHandle {

	public abstract Object getOid();

	public abstract void setOid(long oid);

	public abstract byte getAttrByte(String attrName);

	public abstract boolean getAttrBool(String attrName);

	public abstract short getAttrShort(String attrName);

	public abstract int getAttrInt(String attrName);

	public abstract long getAttrLong(String attrName);

	public abstract char getAttrChar(String attrName);

	public abstract float getAttrFloat(String attrName);

	public abstract double getAttrDouble(String attrName);

	public abstract String getAttrString(String attrName);

	public abstract Date getAttrDate(String attrName);

	public abstract ZooHandle getAttrRefHandle(String attrName);

	public abstract long getAttrRefOid(String attrName);

	public abstract void remove();
	
	public abstract ZooClass getType();
	
	public abstract Object getJavaObject();
	
}