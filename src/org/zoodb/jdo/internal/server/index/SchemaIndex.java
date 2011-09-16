package org.zoodb.jdo.internal.server.index;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.Serializer;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;

public class SchemaIndex {

	private final List<SchemaIndexEntry> _schemaIndex = new LinkedList<SchemaIndexEntry>();
	private int _indexPage1 = -1;
	private final PageAccessFile _raf;
	private boolean _isDirty;

	private static class FieldIndex {
		private String fName;
		private boolean isUnique;
		private FTYPE fType;
		private int page;
		private AbstractPagedIndex index;
	}

	private enum FTYPE {
		LONG(8, Long.TYPE, "long"),
		INT(4, Integer.TYPE, "int"),
		SHORT(2, Short.TYPE, "short"),
		BYTE(1, Byte.TYPE, "byte"),
		DOUBLE(8, Double.TYPE, "double"),
		FLOAT(4, Float.TYPE, "float"),
		CHAR(2, Character.TYPE, "char"), 
		STRING(8, null, "java.lang.String");
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
	public class SchemaIndexEntry {
		private final long _oid;
		//Do not store classes here! See above.
		//We also do not store the class name, as it uses a lot of space, especially since
		//we do not return pages to FSM except the last one.
		private String _cName;   
		private int _objIndexPage;
		private PagedPosIndex _objIndex;
		private List<FieldIndex> _fieldIndices = new LinkedList<FieldIndex>();
		private int _schemaPage;
		
		/**
		 * Constructor for reading index.
		 */
		private SchemaIndexEntry(PageAccessFile raf) {
			_oid = raf.readLong();
			_objIndexPage = raf.readInt();
		    int nF = raf.readShort();
		    for (int i = 0; i < nF; i++) {
		    	FieldIndex fi = new FieldIndex();
		    	_fieldIndices.add(fi);
		    	fi.fName = raf.readString();
		    	fi.fType = FTYPE.values()[raf.readByte()];
		    	fi.isUnique = raf.readBoolean();
		    	fi.page = raf.readInt();
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
		private SchemaIndexEntry(String cName, int schPage, 
				int schPageOfs, PageAccessFile raf, long oid) {
			_oid = oid;
			_cName = cName;
			_objIndex = PagedPosIndex.newIndex(raf);
		}
		
		private void write(PageAccessFile raf) {
		    raf.writeLong(_oid);
		    raf.writeInt(_objIndexPage);  //no data page yet
		    raf.writeShort((short) _fieldIndices.size());
		    for (FieldIndex fi: _fieldIndices) {
		    	raf.writeString(fi.fName);
		    	raf.writeByte((byte) fi.fType.ordinal());
		    	raf.writeBoolean(fi.isUnique);
		    	raf.writeInt(fi.page);
		    }
		}

		public PagedPosIndex getObjectIndex() {
			// lazy loading
			if (_objIndex == null) {
				_objIndex = PagedPosIndex.loadIndex(_raf, _objIndexPage);
			}
			return _objIndex;
		}

		public long getOID() {
			return _oid;
		}
		
		public AbstractPagedIndex defineIndex(ZooFieldDef field, boolean isUnique) {
			//double check
			if (!field.isPrimitiveType() && !field.isString()) {
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
			return fi.index;
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

		public AbstractPagedIndex getIndex(ZooFieldDef field) {
			Iterator<FieldIndex> iter = _fieldIndices.iterator();
			while (iter.hasNext()) {
				FieldIndex fi = iter.next(); 
				if (fi.fName.equals(field.getName())) {
					if (fi.index == null) {
						if (fi.isUnique) {
							fi.index = new PagedUniqueLongLong(_raf, fi.page);
						} else {
							fi.index = new PagedLongLong(_raf, fi.page);
						}
					}
					return fi.index;
				}
			}
			return null;
		}

		public boolean isUnique(ZooFieldDef field) {
			Iterator<FieldIndex> iter = _fieldIndices.iterator();
			while (iter.hasNext()) {
				FieldIndex fi = iter.next(); 
				if (fi.fName.equals(field.getName())) {
					return fi.isUnique;
				}
			}
			throw new IllegalArgumentException("Index not found for " + field.getName());
		}

		/**
		 * 
		 * @return True if any indices were written.
		 */
		private boolean writeAttrIndices() {
			boolean dirty = false;
			Iterator<FieldIndex> iter = _fieldIndices.iterator();
			while (iter.hasNext()) {
				FieldIndex fi = iter.next();
				//is index loaded?
				if (fi.index != null && fi.index.isDirty()) {
					fi.page = fi.index.write();
					dirty = true;
				}
			}
			return dirty;
		}

		public void setName(String className) {
			_cName = className;
		}
	}

	public SchemaIndex(PageAccessFile raf, int indexPage1, boolean isNew) {
		_isDirty = isNew;
		_raf = raf;
		_indexPage1 = indexPage1;
		if (!isNew) {
			readIndex();
		}
	}
	
	private void readIndex() {
		_raf.seekPageForRead(_indexPage1, true);
		int nIndex = _raf.readInt();
		for (int i = 0; i < nIndex; i++) {
			SchemaIndexEntry entry = new SchemaIndexEntry(_raf);
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
			//write attr indices
			if (e.writeAttrIndices()) {
				markDirty();
			}
		}

		if (!isDirty()) {
			return _indexPage1;
		}

		//now write the index directory
		//we can do this only afterwards, because we need to know the pages of the indices
		_indexPage1 = _raf.allocateAndSeek(true, _indexPage1);

		//TODO we should use a PagedObjectAccess here. This means that we treat schemata as objects,
		//but would also allow proper use of FSM for them. 
		
		//number of indices
		_raf.writeInt(_schemaIndex.size());

		//write the index directory
		for (SchemaIndexEntry e: _schemaIndex) {
			e.write(_raf);
		}

		markClean();

		return _indexPage1;
	}

	public SchemaIndexEntry deleteSchema(long oid) {
		Iterator<SchemaIndexEntry> iter = _schemaIndex.iterator();
		while (iter.hasNext()) {
			SchemaIndexEntry e = iter.next();
			if (e._oid == oid) {
				iter.remove();
				markDirty();
				return e;
			}
		}
		return null;
	}

	private SchemaIndexEntry getSchema(String clsName) {
		//search schema in index
		for (SchemaIndexEntry e: _schemaIndex) {
			if (e._cName.equals(clsName)) {
				return e;
			}
		}
		return null;
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

	public List<SchemaIndexEntry> getSchemata() {
		return Collections.unmodifiableList(_schemaIndex);
	}

	public SchemaIndexEntry addSchemaIndexEntry(String clsName, int schPage,
			int schOffs, long oid) {
		// check if such an entry exists!
		if (getSchema(clsName) != null) {
			throw new JDOFatalDataStoreException("Schema is already defined: " + clsName);
		}
		SchemaIndexEntry entry = new SchemaIndexEntry(clsName, schPage, schOffs, _raf, oid);
		_schemaIndex.add(entry);
		markDirty();
		return entry;
	}
	
    protected final boolean isDirty() {
        return _isDirty;
    }
    
	protected final void markDirty() {
		_isDirty = true;
	}
	
	protected final void markClean() {
		_isDirty = false;
	}
	
	
	
	/**
	 * @param oidIndex 
	 * @return Null, if no matching schema could be found.
	 */
	public ZooClassDef readSchema(String clsName, ZooClassDef defSuper, PagedOidIndex oidIndex) {
		SchemaIndexEntry e = getSchema(clsName);
		if (e == null) {
			return null; //no matching schema found 
		}
		FilePos fp = oidIndex.findOid(e.getOID());
		_raf.seekPage(fp.getPage(), fp.getOffs(), true);
		ZooClassDef def = Serializer.deSerializeSchema(_raf);
		def.associateSuperDef(defSuper);
		def.associateFields();
		//and check for indices
		for (ZooFieldDef f: def.getAllFields()) {
			if (e.getIndex(f) != null) {
				f.setIndexed(true);
				f.setUnique(e.isUnique(f));
			}
		}
		return def;
	}
	
	/**
	 * @param oidIndex 
	 * @return List of all schemata in the database. These are loaded when the database is opened.
	 */
	public Collection<ZooClassDef> readSchemaAll(PagedOidIndex oidIndex) {
		Map<Long, ZooClassDef> ret = new HashMap<Long, ZooClassDef>();
		for (SchemaIndexEntry se: getSchemata()) {
			FilePos fp = oidIndex.findOid(se.getOID());
			_raf.seekPage(fp.getPage(), fp.getOffs(), true);
			ZooClassDef def = Serializer.deSerializeSchema(_raf);
			ret.put( def.getOid(), def );
			se.setName( def.getClassName() );
			se._schemaPage = fp.getPage();
		}
		// assign super classes
		for (ZooClassDef def: ret.values()) {
			if (def.getSuperOID() != 0) {
				def.associateSuperDef( ret.get(def.getSuperOID()) );
			}
		}
		
		//associate fields
		for (ZooClassDef def: ret.values()) {
			def.associateFields();
			//and check for indices
			SchemaIndexEntry se = getSchema(def.getOid());
			for (ZooFieldDef f: def.getAllFields()) {
				if (se.getIndex(f) != null) {
					f.setIndexed(true);
					f.setUnique(se.isUnique(f));
				}
			}
		}

		return ret.values();
	}
	
	public void writeSchema(ZooClassDef sch, boolean isNew, long oid, PagedOidIndex oidIndex) {
		String clsName = sch.getClassName();
		SchemaIndexEntry theSchema = getSchema(sch.getOid());
		
        if (!isNew) {
        	//TODO actually, there should always at least be a new schemaOid.
            throw new UnsupportedOperationException("Schema evolution not supported.");
            //TODO rewrite all schemata on page.
            //TODO support schema evolution (what other changes can there be???)
        }

        if (isNew && theSchema != null) {
			throw new JDOUserException("Schema already defined: " +	clsName);
		}
		if (!isNew && theSchema == null) {
			throw new JDOUserException("Schema not found: " + clsName);
		}

		//allocate page
		//TODO write all schemata on one page (or series of pages)?
		//TODO reuse page?
		int prevPage = 0;
		if (!isNew) {
			prevPage = theSchema._schemaPage;
		}
		int schPage = _raf.allocateAndSeek(true, prevPage);
		Serializer.serializeSchema(sch, oid, _raf);

		//Store OID in index
		if (isNew) {
            int schOffs = 0;
            theSchema = addSchemaIndexEntry(clsName, schPage, schOffs, oid);
			oidIndex.insertLong(oid, schPage, schOffs);
		}
	}
	

	public void deleteSchema(ZooClassDef sch, PagedOidIndex oidIndex) {
		//TODO more efficient, do not delete schema (rewriting the index), only flag it as deleted??

		if (!sch.getSubClasses().isEmpty()) {
			//TODO first delete subclasses
			System.out.println("STUB delete subdata pages.");
		}
		
		
		String cName = sch.getClassName();
		SchemaIndexEntry entry = deleteSchema(sch.getOid());
		if (entry == null) {
			throw new JDOUserException("Schema not found: " + cName);
		}

		//update OIDs
		oidIndex.removeOid(sch.getOid());
		
		//TODO remove from FSM

		//delete all associated data
//		int dataPage = entry._dataPage;
		System.out.println("STUB delete data pages.");
//		try {
//			while (dataPage != 0) {
//				_raf.seekPage(dataPage);
//				//dataPage = _raf.readInt();
//				//TODO
//				//store oid/len/data OR store list of OIDS in the beginning of the page?
//				//for now: search OIDS for matching instances?
//				//TODO
//				//remove serialized schema from DB and clean up the page it was located on
//				//->  List<Schema> list;
//				//->  list.addAll(schema on page, except the von to delete)
//				//->  rewrite page
//			}
//		} catch (IOException e) {
//			throw new JDOFatalDataStoreException("Error deleting instances: " + cName, e);
//		}
	}

}
