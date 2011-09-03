package org.zoodb.jdo.internal.client;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooFieldDef;

/**
 * Super class for schema and index operations.
 * 
 * @author Tilmann Zäschke
 *
 */
public abstract class SchemaOperation {

	abstract void preCommit();
	abstract void commit();
	abstract void rollback();

	/**
	 * Operation to create an index.
	 */
	public static class IndexCreate extends SchemaOperation {
		private final Node node;
		private final ZooFieldDef field;
		private final boolean isUnique;

		public IndexCreate(Node node, ZooFieldDef field, boolean isUnique) {
			this.node = node;
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
		private final Node node;
		private final ZooFieldDef field;
		private final boolean isUnique;

		public IndexRemove(Node node, ZooFieldDef field) {
			this.node = node;
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
}
