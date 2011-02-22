package org.zoodb.jdo.internal.server.index;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.server.PageAccessFile;

public class SchemaIndex extends AbstractIndex {

	private final List<SchemaIndexEntry> _schemaIndex = new LinkedList<SchemaIndexEntry>();
	private int _indexPage1 = -1;

	private static class FieldIndex {
		String fName;
		boolean isUnique;
		FTYPE fType;
		long page;
		AbstractIndex index;
	}

	private enum FTYPE {
		LONG(8, Long.TYPE, "long"),
		INT(4, Integer.TYPE, "int"),
		SHORT(2, Short.TYPE, "short"),
		BYTE(1, Byte.TYPE, "byte"),
		DOUBLE(8, Double.TYPE, "double"),
		FLOAT(4, Float.TYPE, "float"),
		CHAR(2, Character.TYPE, "char");
		private final int len;
		private final Type type;
		private final String typeName;
		private FTYPE(int len, Type type, String typeName) {
			this.len = len;
			this.type = type;
			this.typeName = typeName;
		}
		static FTYPE fromType(Type type) {
			for (FTYPE t: values()) {
				if (t.type == type) {
					return t;
				}
			}
			throw new JDOUserException("Type is not indexable: " + type);
		}
		static FTYPE fromType(String typeName) {
			for (FTYPE t: values()) {
				if (t.typeName.equals(typeName)) {
					return t;
				}
			}
			throw new JDOUserException("Type is not indexable: " + typeName);
		}
	}
	
	/**
	 * Do not store classes here. On the server, the class may not be available.
	 * 
	 * Otherwise it would be nice, because comparison of references to classes is faster than
	 * Strings, and references require much less space. Then again, there are few schema classes,
	 * so space is not a problem. 
	 */
	public static class SchemaIndexEntry {
		private final PageAccessFile _raf;
		private final long _oid;
		private final String _cName;  //Do not store classes here! See above. 
		private final int _schemaPage;
		private final int _schemaPageOffset;
		private int _objIndexPage;
		private PagedPosIndex _objIndex;
		private List<FieldIndex> _fieldIndices = new LinkedList<FieldIndex>();
		
		/**
		 * Constructor for reading index.
		 */
		public SchemaIndexEntry(PageAccessFile raf) {
			_raf = raf;
			_oid = raf.readLong();
			_cName = raf.readString();
			_objIndexPage = raf.readInt();
			_schemaPage = raf.readInt();
			_schemaPageOffset = raf.readInt();
		    int nF = raf.readShort();
		    for (int i = 0; i < nF; i++) {
		    	FieldIndex fi = new FieldIndex();
		    	_fieldIndices.add(fi);
		    	fi.fName = raf.readString();
		    	fi.fType = FTYPE.values()[raf.readByte()];
		    	fi.isUnique = raf.readBoolean();
		    	fi.page = raf.readLong();
		    }
		}
		
		/**
		 * Constructor for creating new Index.
		 * @param id
		 * @param cName
		 * @param schPage
		 * @param schPageOfs
		 * @param raf
		 * @throws IOException 
		 */
		public SchemaIndexEntry(String cName, int schPage, 
				int schPageOfs, PageAccessFile raf, long oid) {
			_raf = raf;
			_oid = oid;
			_cName = cName;
			_schemaPage = schPage;
			_schemaPageOffset = schPageOfs;
			_objIndex = PagedPosIndex.newIndex(raf);
		}
		
		public static SchemaIndexEntry read(PageAccessFile raf) {
			return new SchemaIndexEntry(raf);
		}

		private void write(PageAccessFile raf) {
		    raf.writeLong(_oid);
		    raf.writeString(_cName);
		    raf.writeInt(_objIndexPage);  //no data page yet
		    raf.writeInt(_schemaPage);
		    raf.writeInt(_schemaPageOffset);
		    raf.writeShort((short) _fieldIndices.size());
		    for (FieldIndex fi: _fieldIndices) {
		    	raf.writeString(fi.fName);
		    	raf.writeByte((byte) fi.fType.ordinal());
		    	raf.writeBoolean(fi.isUnique);
		    	raf.writeLong(fi.page);
		    }
		}

		public PagedPosIndex getObjectIndex() {
			// lazy loading
			if (_objIndex == null) {
				_objIndex = PagedPosIndex.loadIndex(_raf, _objIndexPage);
			}
			return _objIndex;
		}

		public int getPage() {
			return _schemaPage;
		}

		public int getOffset() {
			return _schemaPageOffset;
		}

		public String getSchemaName() {
			return _cName;
		}

		public void defineIndex(ZooClassDef cls, ZooFieldDef field,
				boolean isUnique) {
			//double check
			if (!field.isPrimitiveType()) {
				throw new JDOUserException("Type can not be indexed: " + field.getTypeName());
			}
			for (FieldIndex fi: _fieldIndices) {
				if (fi.fName.equals(field.getName())) {
					throw new JDOUserException("Index is already defined: " + field.getName());
				}
			}
			FieldIndex fi = new FieldIndex();
			fi.fName = field.getName();
			fi.fType = FTYPE.fromType(field.getTypeName());
			fi.isUnique = isUnique;
			field.setIndexed(true);
			field.setUnique(isUnique);
			if (isUnique) {
				fi.index = new PagedUniqueLongLong(_raf);
			} else {
				fi.index = new PagedLongLong(_raf);
			}
			_fieldIndices.add(fi);
		}

		public boolean removeIndex(ZooFieldDef field) {
			Iterator<FieldIndex> iter = _fieldIndices.iterator();
			while (iter.hasNext()) {
				FieldIndex fi = iter.next(); 
				if (fi.fName.equals(field.getName())) {
					iter.remove();
					field.setIndexed(false);
					return true;
				}
			}
			return false;
		}
	}

	public SchemaIndex(PageAccessFile raf, int indexPage1, boolean isNew) {
		super(raf, isNew, true);
		_indexPage1 = indexPage1;
		if (!isNew) {
			readIndex();
		}
	}
	
	private void readIndex() {
		_raf.seekPage(_indexPage1, true);
		int nIndex = _raf.readInt();
		for (int i = 0; i < nIndex; i++) {
			SchemaIndexEntry entry = SchemaIndexEntry.read(_raf);
			_schemaIndex.add(entry);
		}
	}

	
	public int write() {
		//write the indices
		for (SchemaIndexEntry e: _schemaIndex) {
			if (e._objIndex != null) {
				int p = e.getObjectIndex().write();
				if (p != e._objIndexPage) {
					markDirty();
				}
				e._objIndexPage = p;
			}
		}

		if (!isDirty()) {
			return _indexPage1;
		}

		//now write the index directory
		//we can do this only afterwards, because we need to know the pages of the indices
		_indexPage1 = _raf.allocateAndSeek(true);

		//number of indices
		_raf.writeInt(_schemaIndex.size());

		//write the index directory
		for (SchemaIndexEntry e: _schemaIndex) {
			e.write(_raf);
		}

		markClean();

		return _indexPage1;
	}

	public SchemaIndexEntry deleteSchema(String cName) {
		Iterator<SchemaIndexEntry> iter = _schemaIndex.iterator();
		while (iter.hasNext()) {
			SchemaIndexEntry e = iter.next();
			if (e._cName.equals(cName)) {
				iter.remove();
				markDirty();
				return e;
			}
		}
		return null;
	}

	public SchemaIndexEntry getSchema(String clsName) {
		//search schema in index
		for (SchemaIndexEntry e: _schemaIndex) {
			if (e._cName.equals(clsName)) {
				return e;
			}
		}
		return null;
	}
	
	public void add(SchemaIndexEntry entry) {
		// check if such an entry exists!
		if (getSchema(entry._cName) != null) {
			throw new JDOFatalDataStoreException("Schema is already defined: " + entry._cName);
		}
		_schemaIndex.add(entry);
		markDirty();
	}

	public List<SchemaIndexEntry> objIndices() {
		return _schemaIndex;
	}

	public SchemaIndexEntry getSchema(long oid) {
		//search schema in index
		for (SchemaIndexEntry e: _schemaIndex) {
			if (e._oid == oid) {
				return e;
			}
		}
		return null;
	}
}
