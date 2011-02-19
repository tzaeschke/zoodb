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
	
	public boolean isSchemaDefined(Class<?> type, Node node) {
		//is it in cache?
		if (_cache.isSchemaDefined(type, node)) {
			return true;
		}
		
		//first load super types
		//-> if (cls==PersCapableCls) then supClsDef = null
		ZooClassDef supClsDef = null;
		if (PersistenceCapableImpl.class != type) {
			Class<?> sup = type.getSuperclass();
			 supClsDef = locateClassDefinition(sup, node);
		} else {
			supClsDef = null;
		}
		
		//is it in the DB?
		ZooClassDef clsDef = node.loadSchema(type.getName(), supClsDef);
		if (clsDef == null) {
			return false;
		}
		_cache.addSchema(clsDef, true, node);
		return true;
	}

	/**
	 * Checks class and disk for class definition.
	 * @param cls
	 * @param node
	 * @return Class definition, may return null if no definition is found.
	 */
	private ZooClassDef locateClassDefinition(Class<?> cls, Node node) {
		//first load super types
		//-> if (cls==PersCapableCls) then supClsDef = null
		ZooClassDef supClsDef = null;
		if (PersistenceCapableImpl.class != cls) {
			Class<?> sup = cls.getSuperclass();
			supClsDef = locateClassDefinition(sup, node);
		} else {
			supClsDef = null;
		}
		
		//TODO we are searching twice through the cache here....
		CachedSchema cs = _cache.findCachedSchema(cls, node);
		ZooClassDef def = null;
		if (cs != null) {
			//return null if deleted
			if (!cs.isDeleted()) { //TODO load if hollow???
				def = cs.getSchema();
			}
		} else {
			def = node.loadSchema(cls.getName(), supClsDef);
			if (def != null) {
				_cache.addSchema(def, true, node);
			}
		}
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
		//TODO USe classNames internally, rather than classes
		//TODO remove:
		try {
			Class<?> cls = Class.forName(className);
			return locateSchema(cls, node);
		} catch (ClassNotFoundException e) {
			throw new JDOUserException("Class not found: " + className, e);
		}
	}

	public ISchema createSchema(Node node, Class<?> cls, boolean isLoaded) {
		if (!isLoaded && isSchemaDefined(cls, node)) {
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
		Class<?> clsSuper = null;
		ZooClassDef defSuper = null;
		if (cls != PersistenceCapableImpl.class) {
			clsSuper = cls.getSuperclass();
			defSuper = locateClassDefinition(clsSuper, node);
		}
		long oid = node.getOidBuffer().allocateOid();
		ZooClassDef def = new ZooClassDef(cls, oid, defSuper); 
		_cache.addSchema(def, isLoaded, node);
		return new ISchema(def, cls, node, this);
	}

	public void deleteSchema(ISchema iSchema) {
		// TODO Auto-generated method stub
		System.out.println("FIXME SchemaManager.deleteSchema(): check fur sub-classes.");
		Class<?> cls = iSchema.getSchemaClass();
		Node node = iSchema.getNode();
		CachedSchema cs = _cache.findCachedSchema(cls, node);
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
		for (ZooFieldDef f: def.getFields()) {
			if (f.getName().equals(fieldName)) {
				node.defineIndex(def, f, isUnique);
				return;
			}
		}
		throw new JDOUserException("Field name not found: " + fieldName);
	}
}
