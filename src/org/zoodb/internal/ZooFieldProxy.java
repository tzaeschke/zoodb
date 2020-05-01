/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal;

import org.zoodb.internal.client.SchemaManager;
import org.zoodb.schema.ZooField;
import org.zoodb.schema.ZooHandle;

/**
 * The class serves as a proxy for the latest version of a particular class in the schema version
 * tree.
 * The proxy's reference to the latest version is updated by SchemaOperations.
 * 
 * @author ztilmann
 */
public class ZooFieldProxy implements ZooField {

	private boolean isInvalid = false;
	private ZooFieldDef fieldDef;
	private final SchemaManager schemaManager;

	ZooFieldProxy(ZooFieldDef fieldDef, SchemaManager schemaManager) {
		this.fieldDef = fieldDef;
		this.schemaManager = schemaManager;
	}

	@Override
	public String toString() {
		checkInvalidRead();
		return "Class schema field: " + fieldDef.getName();
	}

	private void checkInvalidRead() {
		checkInvalid(false);
	}

	private void checkInvalidWrite() {
		checkInvalid(true);
	}

	private void checkInvalid(boolean write) {
		Session s = fieldDef.getDeclaringType().getProvidedContext().getSession();
		if (s.isClosed()) {
			throw new IllegalStateException("This schema belongs to a closed PersistenceManager.");
		}
		if (!s.isActive()) {
			if (write || !s.getConfig().getNonTransactionalRead()) {
				throw new IllegalStateException("The transaction is currently not active.");
			}
		}
		if (isInvalid) {
			throw new IllegalStateException("This schema field object is invalid, for " +
					"example because it has been deleted.");
		}
	}

	@Override
	public void remove() {
		checkInvalidWrite();
		isInvalid = true;
		fieldDef.getDeclaringType().getVersionProxy().removeField(this);
	}

	@Override
	public void rename(String fieldName) {
		checkInvalidWrite();
		ZooClassProxy.checkJavaFieldNameConformity(fieldName);
		if (fieldDef.getDeclaringType().getVersionProxy().getField(fieldName) != null) {
			throw new IllegalArgumentException("Field name already taken: " + fieldName);
		}
		schemaManager.renameField(fieldDef, fieldName);
	}

	@Override
	public String getName() {
		checkInvalidRead();
		return fieldDef.getName();
	}

	public ZooFieldDef getFieldDef() {
		return fieldDef;
	}

	public void updateVersion(ZooFieldDef newFieldDef) {
		fieldDef = newFieldDef;
	}

    @Override
    public Object getValue(ZooHandle hdl) {
		checkInvalidRead();
		ZooHandleImpl h = checkHandle(hdl);
		h.getGenericObject().activateRead();
        return h.getGenericObject().getField(fieldDef);
    }
    
    @Override
    public void setValue(ZooHandle hdl, Object val) {
		checkInvalidWrite();
		ZooHandleImpl h = checkHandle(hdl);
        h.getGenericObject().jdoZooMarkDirty();
        h.getGenericObject().setField(fieldDef, val);
    }

    private ZooHandleImpl checkHandle(ZooHandle hdl) {
    	ZooHandleImpl hdlI = (ZooHandleImpl) hdl;
    	ZooClassProxy c = (ZooClassProxy) hdlI.getType();
    	if (!fieldDef.getDeclaringType().isSuperTypeOf(c.getSchemaDef())) {
    		throw new IllegalArgumentException("Field '" + fieldDef.getName() + 
    				"' is not present in " + c.getSchemaDef().getClassName());
    	}
    	if (hdlI.getGenericObject().jdoZooIsDeleted()) {
    		throw new IllegalStateException("The handle has been deleted.");
    	}
    	return hdlI;
    }
    
	@Override
	public String getTypeName() {
		checkInvalidRead();
		return fieldDef.getTypeName();
	}

	@Override
	public void createIndex(boolean isUnique) {
		checkInvalidWrite();
		schemaManager.defineIndex(fieldDef, isUnique);
	}

	@Override
	public boolean removeIndex() {
		checkInvalidWrite();
		return schemaManager.removeIndex(fieldDef);
	}

	@Override
	public boolean hasIndex() {
		checkInvalidRead();
		return schemaManager.isIndexDefined(fieldDef);
	}

	@Override
	public boolean isIndexUnique() {
		checkInvalidRead();
		return schemaManager.isIndexUnique(fieldDef);
	}

	@Override
	public int getArrayDim() {
		return fieldDef.getArrayDim();
	}

	public void invalidate() {
		isInvalid = true;
	}
}
