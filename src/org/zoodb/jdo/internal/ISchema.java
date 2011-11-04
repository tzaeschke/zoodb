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
