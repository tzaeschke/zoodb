package org.zoodb.jdo.internal;

import javax.jdo.JDOUserException;

import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.internal.client.SchemaManager;

/**
 * Internal Schema class.
 * 
 * @author Tilmann Zäschke
 */
public class ISchema extends ZooSchema {

	private final ZooClassDef _def;
	private final Node _node;
	private boolean _isInvalid = false;
	private final SchemaManager _schemaManager;
	
	public ISchema(ZooClassDef def, Class<?> cls, Node node, SchemaManager schemaManager) {
		super(cls);
		_def = def;
		_node = node;
		_schemaManager = schemaManager;
	}

	@Override
	public void remove() {
		checkInvalid();
		_schemaManager.deleteSchema(this);
		invalidate();
	}

	public ZooClassDef getSchemaDef() {
		checkInvalid();
		return _def;
	}

	public Node getNode() {
		checkInvalid();
		return _node;
	}
	
	/**
	 * Call this for example when the objects is deleted.
	 * This is irreversible. Even after rollback(), user should get a new
	 * Schema object.
	 */
	private void invalidate() {
		//The alternative would have been to invalidate it during commit
		//and revalidate it during rollback(), only if there was not
		//commit() before the rollback() -> complicated.
		//Furthermore it would have been valid until the commit, beyond
		//any call to deleteSchema().
		_isInvalid = true;
	}
	
	protected void checkInvalid() {
		if (_isInvalid) {
			throw new JDOUserException("This schema object is invalid, for " +
					"example because it has been deleted.");
		}
	}
	
	@Override
	public void defineIndex(String fieldName, boolean isUnique) {
		checkInvalid();
		_schemaManager.defineIndex(fieldName, isUnique, _node, _def);
	}
	
	@Override
	public boolean removeIndex(String fieldName) {
		checkInvalid();
		return _schemaManager.removeIndex(fieldName, _node, _def);
	}
	
	@Override
	public boolean isIndexDefined(String fieldName) {
		checkInvalid();
		return _schemaManager.isIndexDefined(fieldName, _node, _def);
	}
	
	@Override
	public boolean isIndexUnique(String fieldName) {
		checkInvalid();
		return _schemaManager.isIndexUnique(fieldName, _node, _def);
	}
	
	@Override
	public void dropInstances() {
		checkInvalid();
		_schemaManager.dropInstances(_node, _def);
	}
}
