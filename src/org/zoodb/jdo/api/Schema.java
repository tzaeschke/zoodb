package org.zoodb.jdo.api;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;


public abstract class Schema {

	protected Class<?> _cls;

	public Schema(Class<?> cls) {
		_cls = cls;
	}
	
	public static Schema create(
			PersistenceManager pm, Class<?> cls, String nodeName) {
		Node node = Session.getSession(pm).getNode(nodeName);
		return Session.getSession(pm).getSchemaManager().createSchema(node, cls, false);
	}

	public static Schema locate(
			PersistenceManager pm, Class<?> cls, String nodeName) {
		Node node = Session.getSession(pm).getNode(nodeName);
		return Session.getSession(pm).getSchemaManager().locateSchema(cls, node);
	}

	public Class<?> getSchemaClass() {
		checkInvalid();
		return _cls;
	}

	@Override
	public String toString() {
		checkInvalid();
		return "Schema: " + _cls.getName();
	}
	
	public static Schema locate(PersistenceManager pm, String className,
			String nodeName) {
		Node node = Session.getSession(pm).getNode(nodeName);
		return Session.getSession(pm).getSchemaManager().locateSchema(className, node);
	}

	public abstract void remove();
	
	protected abstract void checkInvalid();
	
	public abstract void defineIndex(String fieldName, boolean isUnique);
	
	public abstract void removeIndex(String fieldName);
	
	public abstract boolean isIndexDefined(String fieldName);
	
	public abstract boolean isIndexUnique(String fieldName);
}
