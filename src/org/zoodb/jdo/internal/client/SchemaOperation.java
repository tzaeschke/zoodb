package org.zoodb.jdo.internal.client;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;

/**
 * Super class for schema and index operations.
 * 
 * This is important for example for creating indices, which we should do only during commit,
 * instead of as soon as the index creation method is called by the user. Therefore we need special
 * messages that can be sent to the storage engine.
 * To get this working even if the schema itself is not committed yet, schema creation and deletion
 * has to become an operation as well. 
 * These messages also need to be ordered, which is another reason why they can not be treated
 * as normal objects.
 * 
 * However schemata are objects, so they get also treated by the normal commit procedure, so we
 * have to take care that we don't interfere with normal commit when we execute these schema 
 * operations.
 * 
 * Finally, we need to perform all operations before objects are committed to ensure that the
 * required schemata are already present in the database.
 * 
 * TODO If we implement this for adding/removing schema as well, we should treat them even more like
 * normal objects in the commit-procedure, no special treatment should nenecessary anymore.
 * 
 * @author Tilmann Zäschke
 *
 */
public abstract class SchemaOperation {

	protected final Node node;
	
	private SchemaOperation(Node node) {
		this.node = node;
	}
	
	abstract void preCommit();
	abstract void commit();
	abstract void rollback();

	/**
	 * Operation to create an index.
	 */
	public static class IndexCreate extends SchemaOperation {
		private final ZooFieldDef field;
		private final boolean isUnique;

		public IndexCreate(Node node, ZooFieldDef field, boolean isUnique) {
			super(node);
			this.field = field;
			this.isUnique = isUnique;
			preCommit();
		}
		
		@Override
		void preCommit() {
			field.setIndexed(true);
			field.setUnique(isUnique);
		}
		
		@Override
		void commit() {
			node.defineIndex(field.getDeclaringType(), field, isUnique);
		}
		
		@Override
		void rollback() {
			field.setIndexed(false);
		}
	}
	
	/**
	 * Operation to remove an index.
	 */
	public static class IndexRemove extends SchemaOperation {
		private final ZooFieldDef field;
		private final boolean isUnique;

		public IndexRemove(Node node, ZooFieldDef field) {
			super(node);
			this.field = field;
			this.isUnique = field.isIndexUnique();
			preCommit();
		}
		
		@Override
		void preCommit() {
			field.setIndexed(false);
		}
		
		@Override
		void commit() {
			node.removeIndex(field.getDeclaringType(), field);
		}
		
		@Override
		void rollback() {
			field.setIndexed(true);
			field.setUnique(isUnique);
		}
	}

	public static class DropInstances extends SchemaOperation {
		private final ZooClassDef def;

		public DropInstances(Node node, ZooClassDef def) {
			super(node);
			this.def = def;
			preCommit();
		}

		@Override
		void preCommit() {
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

		public SchemaDefine(Node node, ZooClassDef def) {
			super(node);
			this.def = def;
			preCommit();
		}

		@Override
		void preCommit() {
			//Nothing to do?
		}

		@Override
		void commit() {
			node.defineSchema(def);
		}

		@Override
		void rollback() {
			//Nothing to do?		
		}
	}


	public static class SchemaDelete extends SchemaOperation {
		private final ZooClassDef def;

		public SchemaDelete(Node node, ZooClassDef def) {
			super(node);
			this.def = def;
			preCommit();
		}

		@Override
		void preCommit() {
			//Nothing to do?
		}

		@Override
		void commit() {
			node.undefineSchema(def);
		}

		@Override
		void rollback() {
			//Nothing to do?		
		}
	}
}
