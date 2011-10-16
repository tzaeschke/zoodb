package org.zoodb.jdo.api;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooHandle;


/**
 * Public interface to manage database schemata.
 * 
 * @author ztilmann
 */
public abstract class ZooSchema {

	protected Class<?> cls;

	public ZooSchema(Class<?> cls) {
		this.cls = cls;
	}
	
	public static ZooSchema create(PersistenceManager pm, Class<?> cls) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().createSchema(node, cls);
	}

	public static ZooSchema locate(PersistenceManager pm, Class<?> cls) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().locateSchema(cls, node);
	}

	public static ZooSchema create(
			PersistenceManager pm, Class<?> cls, String nodeName) {
		Node node = Session.getSession(pm).getNode(nodeName);
		return Session.getSession(pm).getSchemaManager().createSchema(node, cls);
	}

	public static ZooSchema locate(
			PersistenceManager pm, Class<?> cls, String nodeName) {
		Node node = Session.getSession(pm).getNode(nodeName);
		return Session.getSession(pm).getSchemaManager().locateSchema(cls, node);
	}

	public Class<?> getSchemaClass() {
		checkInvalid();
		return cls;
	}

	@Override
	public String toString() {
		checkInvalid();
		return "Schema: " + cls.getName();
	}
	
	public static ZooSchema locate(PersistenceManager pm, String className,
			String nodeName) {
		Node node = Session.getSession(pm).getNode(nodeName);
		return Session.getSession(pm).getSchemaManager().locateSchema(className, node);
	}

	public static ZooSchema locate(PersistenceManager pm, String className) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().locateSchema(className, node);
	}

	public abstract void remove();
	
	protected abstract void checkInvalid();
	
	public abstract void defineIndex(String fieldName, boolean isUnique);
	
	public abstract boolean removeIndex(String fieldName);
	
	public abstract boolean isIndexDefined(String fieldName);
	
	public abstract boolean isIndexUnique(String fieldName);

	public static ZooHandle getHandle(PersistenceManager pm, long oid) {
		return Session.getSession(pm).getHandle(oid);
	}

	/**
	 * Drops all instances of the class. This does not affect cached instances
	 */
	public abstract void dropInstances();

//	public abstract byte getAttrByte(String attrName);
//	public abstract boolean getAttrBool(String attrName);
//	public abstract short getAttrShort(String attrName);
//	public abstract int getAttrInt(String attrName);
//	public abstract long getAttrLong(String attrName);
//	public abstract char getAttrChar(String attrName);
//	public abstract float getAttrFloat(String attrName);
//	public abstract double getAttrDouble(String attrName);
//	public abstract String getAttrString(String attrName);
//	public abstract ZooHandle getAttrRefHandle(String attrName);
}
