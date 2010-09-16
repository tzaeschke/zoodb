package org.zoodb.jdo.internal;

import javax.jdo.JDOUserException;

import org.zoodb.jdo.api.Schema;
import org.zoodb.jdo.internal.client.SchemaManager;

/**
 * Internal Schema class.
 * 
 * @author Tilmann Zäschke
 */
public class ISchema extends Schema {

	private ZooClassDef _def;
	private Node _node;
	private boolean _isInvalid = false;
	private SchemaManager _schemaManager;
	
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
	public void invalidate() {
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
}
