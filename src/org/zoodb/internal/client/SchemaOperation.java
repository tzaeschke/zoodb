/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.client;

import org.zoodb.internal.Node;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.client.session.ClientSessionCache;

/**
 * Super class for schema and index operations.
 * 
 * This is important for example for creating indices, which we should do only during commit,
 * instead of as soon as the index creation method is called by the user. Therefore we need special
 * messages that can be sent to the storage engine.
 * To get this working even if the schema itself is not committed yet, schema creation and deletion
 * has to become an operation as well. 
 * These messages also need to be ordered, which is another reason why they cannot be treated
 * as normal objects.
 * 
 * However schemata are objects, so they get also treated by the normal commit procedure, so we
 * have to take care that we don't interfere with normal commit when we execute these schema 
 * operations.
 * 
 * Finally, we need to perform all operations before objects are committed to ensure that the
 * required schemata are already present in the database.
 * 
 * TODO If we implement this for adding/removing schema as well, we should treat them even more 
 * like normal objects in the commit-procedure, no special treatment should be necessary anymore.
 * 
 * @author Tilmann Zaeschke
 *
 */
public abstract class SchemaOperation {

	protected final Node node;
	
	private SchemaOperation(Node node) {
		this.node = node;
	}
	
	abstract void initial();
	abstract void commit();
	abstract void rollback();

	/**
	 * Operation to create an index.
	 */
	public static class IndexCreate extends SchemaOperation {
		private final ZooFieldDef field;
		private final boolean isUnique;

		public IndexCreate(ZooFieldDef field, boolean isUnique) {
			super(field.getDeclaringType().jdoZooGetNode());
			this.field = field;
			this.isUnique = isUnique;
			initial();
		}
		
		@Override
		void initial() {
			field.setIndexed(true);
			field.setUnique(isUnique);
			ZooClassDef def = field.getDeclaringType(); 
			def.getProvidedContext().getIndexer().refreshWithSchema(def);
		}
		
		@Override
		void commit() {
			node.defineIndex(field.getDeclaringType(), field, isUnique);
		}
		
		@Override
		void rollback() {
			field.setIndexed(false);
			ZooClassDef def = field.getDeclaringType(); 
			def.getProvidedContext().getIndexer().refreshWithSchema(def);
		}
	}
	
	/**
	 * Operation to remove an index.
	 */
	public static class IndexRemove extends SchemaOperation {
		private final ZooFieldDef field;
		private final boolean isUnique;

		public IndexRemove(ZooFieldDef field) {
			super(field.getDeclaringType().jdoZooGetNode());
			this.field = field;
			this.isUnique = field.isIndexUnique();
			initial();
		}
		
		@Override
		void initial() {
			field.setIndexed(false);
			ZooClassDef def = field.getDeclaringType(); 
			def.getProvidedContext().getIndexer().refreshWithSchema(def);
		}
		
		@Override
		void commit() {
			node.removeIndex(field.getDeclaringType(), field);
		}
		
		@Override
		void rollback() {
			field.setIndexed(true);
			field.setUnique(isUnique);
			ZooClassDef def = field.getDeclaringType(); 
			def.getProvidedContext().getIndexer().refreshWithSchema(def);
		}
	}

	public static class DropInstances extends SchemaOperation {
		private final ZooClassProxy def;

		public DropInstances(ZooClassProxy def) {
			super(def.getSchemaDef().jdoZooGetNode());
			this.def = def;
			initial();
		}

		@Override
		void initial() {
			// Nothing to do
		}

		@Override
		void commit() {
			node.dropInstances(def);
		}

		@Override
		void rollback() {
			// Nothing to do
		}

	}
	
	
	public static class SchemaDefine extends SchemaOperation {
		private final ZooClassDef def;

		public SchemaDefine(ZooClassDef def) {
			super(def.jdoZooGetNode());
			this.def = def;
			initial();
		}

		@Override
		void initial() {
			//Nothing to do?
		}

		@Override
		void commit() {
			node.defineSchema(def);
		}

		@Override
		void rollback() {
			def.getVersionProxy().socRemoveDef();
			def.getVersionProxy().invalidate();
		}
	}


	public static class SchemaRename extends SchemaOperation {
		private final ZooClassDef def;
		private final String newName;
		private final String oldName;
		private final ClientSessionCache cache;

		public SchemaRename(ClientSessionCache cache, ZooClassDef def, String newName) {
			super(def.jdoZooGetNode());
			this.def = def;
			this.newName = newName;
			this.oldName = def.getClassName();
			this.cache = cache;
			initial();
		}

		@Override
		void initial() {
			Class<?> oldCls = def.getJavaClass();
			def.rename(newName);
			cache.updateSchema(def, oldCls, def.getJavaClass());
		}

		@Override
		void commit() {
			def.getProvidedContext().getNode().renameSchema(def, newName);
		}

		@Override
		void rollback() {
			Class<?> oldCls = def.getJavaClass();
			def.rename(oldName);
			cache.updateSchema(def, oldCls, def.getJavaClass());
		}
	}


	public static class SchemaDelete extends SchemaOperation {
		private final ZooClassProxy def;

		public SchemaDelete(ZooClassProxy def) {
			super(def.getSchemaDef().jdoZooGetNode());
			this.def = def;
			initial();
		}

		@Override
		void initial() {
		    def.socRemoveDef();
		}

		@Override
		void commit() {
			node.undefineSchema(def);
		}

		@Override
		void rollback() {
		    def.socRemoveDefRollback();
		}
	}

	public static class SchemaFieldDefine extends SchemaOperation {
		private final ZooClassDef cls;
		private final ZooFieldDef field;

		public SchemaFieldDefine(ZooClassDef cls, ZooFieldDef field) {
			super(cls.jdoZooGetNode());
			this.cls = cls;
			this.field = field;
			initial();
		}

		@Override
		void initial() {
			//TODO roll back to previous version instance???
		    cls.addField(field);
		}

		@Override
		void commit() {
			//nothing to do?
		}

		@Override
		void rollback() {
			cls.removeField(field);
			field.getProxy().invalidate();
		}

		public ZooFieldDef getField() {
			return field;
		}
	}


	public static class SchemaFieldRename extends SchemaOperation {
		private final ZooFieldDef field;
		private final String newName;
		private final String oldName;

		public SchemaFieldRename(ZooFieldDef field, String newName) {
			super(field.getDeclaringType().jdoZooGetNode());
			this.field = field;
			this.newName = newName;
			this.oldName = field.getName();
			initial();
		}

		@Override
		void initial() {
			field.updateName(newName);
		}

		@Override
		void commit() {
			//nothing to do?
		}

		@Override
		void rollback() {
			field.updateName(oldName);
		}
	}


	public static class SchemaFieldDelete extends SchemaOperation {
		private final ZooClassDef cls;
		private final ZooFieldDef field;

		public SchemaFieldDelete(ZooClassDef cls, ZooFieldDef field) {
			super(cls.jdoZooGetNode());
			this.cls = cls;
			this.field = field;
			initial();
		}

		@Override
		void initial() {
		    cls.removeField(field);
		}

		@Override
		void commit() {
			// nothing to do?
		}

		@Override
		void rollback() {
			//TODO roll back to old version???
		    cls.addField(field);
		}
		
		public ZooFieldDef getField() {
			return field;
		}
	}

	
	/**
	 * This operation creates a new version in the schema version tree. 
	 */
	public static class SchemaNewVersion extends SchemaOperation {

		private final ZooClassDef defOld;
		private final ZooClassDef defNew;
		private final ClientSessionCache cache;
		
		public SchemaNewVersion(ZooClassDef defOld, ZooClassDef defNew, ClientSessionCache cache) {
			super(defOld.getProvidedContext().getNode());
			this.defOld = defOld;
			this.defNew = defNew;
			this.cache = cache;
			initial();
		}

		@Override
		void initial() {
			// nothing to do?
		}

		@Override
		void commit() {
			node.newSchemaVersion(defNew);
		}

		@Override
		void rollback() {
			defOld.newVersionRollback(defNew, cache);
		}

	}
}
