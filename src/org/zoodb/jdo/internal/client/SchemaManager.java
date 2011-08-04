package org.zoodb.jdo.internal.client;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.ISchema;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.CachedObject.CachedSchema;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.DatabaseLogger;

/**
 * This class maps schema data between the external Schema/ISchema classes and
 * the internal ZooClassDef class. 
 * 
 * @author Tilmann Zäschke
 *
 */
public class SchemaManager {

	private ClientSessionCache _cache;

	public SchemaManager(ClientSessionCache cache) {
		_cache = cache;
	}
	
	public boolean isSchemaDefined(Class<?> cls, Node node) {
		return (locateClassDefinition(cls, node) != null);
	}

	/**
	 * Checks class and disk for class definition.
	 * @param cls
	 * @param node
	 * @return Class definition, may return null if no definition is found.
	 */
	private ZooClassDef locateClassDefinition(Class<?> cls, Node node) {
		CachedSchema cs = _cache.getCachedSchema(cls, node);
		if (cs != null) {
			//return null if deleted
			if (!cs.isDeleted()) { //TODO load if hollow???
				return cs.getSchema();
			}
			return null;
		}
		
		//first load super types
		//-> if (cls==PersCapableCls) then supClsDef = null
		ZooClassDef supClsDef = null;
		if (PersistenceCapableImpl.class != cls) {
			Class<?> sup = cls.getSuperclass();
			supClsDef = locateClassDefinition(sup, node);
		} else {
			supClsDef = null;
		}
		

		DatabaseLogger.debugPrintln(1, "Cache miss for schema: " + cls.getName());
		ZooClassDef def = node.loadSchema(cls.getName(), supClsDef);
		return def;
	}

	public ISchema locateSchema(Class<?> cls, Node node) {
		ZooClassDef def = locateClassDefinition(cls, node);
		//not in cache and not on disk
		if (def == null) {
			return null;
		}
		//it should now be in the cache
		//return a unique handle, even if called multiple times. There is currently
		//no real reason, other than that it allows == comparison.
		ISchema ret = def.getApiHandle();
		if (ret == null) {
			ret = new ISchema(def, cls, node, this);
			def.setApiHandle(ret);
		}
		return ret;
	}

	public ISchema locateSchema(String className, Node node) {
		try {
			Class<?> cls = Class.forName(className);
			return locateSchema(cls, node);
		} catch (ClassNotFoundException e) {
			throw new JDOUserException("Class not found: " + className, e);
		}
	}

	public ISchema createSchema(Node node, Class<?> cls) {
		if (isSchemaDefined(cls, node)) {
			throw new JDOUserException(
					"Schema is already defined: " + cls.getName());
		}
		//Is this PersistentCapanbleImpl or a sub class?
		if (! (PersistenceCapableImpl.class.isAssignableFrom(cls))) {
//???TODO?? -> what is that for??
//			//super class in not PersistentCapableImpl. Check if it is at least 
//			//persistent capable.
//			if (!isSchemaDefined(clsSuper, node)) {
				throw new JDOUserException(
						"Class has no persistent capable super class: " + cls.getName());
//			}
		}
		ZooClassDef def;
		long oid = node.getOidBuffer().allocateOid();
		if (cls != PersistenceCapableImpl.class) {
			Class<?> clsSuper = cls.getSuperclass();
			ZooClassDef defSuper = locateClassDefinition(clsSuper, node);
			def = ZooClassDef.createFromJavaType(cls, oid, defSuper); 
		} else {
			def = ZooClassDef.createFromJavaType(cls, oid, null);
		}
		_cache.addSchema(def, false, node);
		return new ISchema(def, cls, node, this);
	}

	public void deleteSchema(ISchema iSchema) {
		System.out.println("FIXME SchemaManager.deleteSchema(): check fur sub-classes.");
		Class<?> cls = iSchema.getSchemaClass();
		Node node = iSchema.getNode();
		CachedSchema cs = _cache.getCachedSchema(cls, node);
		if (cs == null) {
			throw new IllegalStateException(
					"Schema exists but is not in cache!!! " + cls.getName());
		}
		if (cs.isDeleted()) {
			throw new JDOObjectNotFoundException("This objects has already been deleted.");
		}
		cs.markDeleted();
	}

	public void defineIndex(String fieldName, boolean isUnique, Node node, ZooClassDef def) {
		ZooFieldDef f = getFieldDef(def, fieldName);
		if (f.isIndexed()) {
			throw new JDOUserException("Field is already indexed: " + fieldName);
		}
		node.defineIndex(def, f, isUnique);
	}

	public boolean removeIndex(String fieldName, Node node, ZooClassDef def) {
		ZooFieldDef f = getFieldDef(def, fieldName);
		if (!f.isIndexed()) {
			return false;
		}
		return node.removeIndex(def, f);
	}

	public boolean isIndexDefined(String fieldName, Node node, ZooClassDef def) {
		ZooFieldDef f = getFieldDef(def, fieldName);
		return f.isIndexed();
	}

	public boolean isIndexUnique(String fieldName, Node node, ZooClassDef def) {
		ZooFieldDef f = getFieldDef(def, fieldName);
		if (!f.isIndexed()) {
			throw new JDOUserException("Field has no index: " + fieldName);
		}
		return f.isIndexUnique();
	}
	
	private ZooFieldDef getFieldDef(ZooClassDef def, String fieldName) {
		for (ZooFieldDef f: def.getAllFields()) {
			if (f.getName().equals(fieldName)) {
				return f;
			}
		}
		throw new JDOUserException("Field name not found: " + fieldName + " in " + 
				def.getClassName());
	}
}
