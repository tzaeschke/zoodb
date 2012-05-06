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
package org.zoodb.jdo.internal.server.index;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
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
import org.zoodb.jdo.internal.util.PrimLongMapLI;
import org.zoodb.jdo.internal.util.Util;

public class SchemaIndex {

	private final PrimLongMapLI<SchemaIndexEntry> schemaIndex = 
		new PrimLongMapLI<SchemaIndexEntry>();
	private int pageId = -1;
	private final PageAccessFile raf;
	private boolean isDirty = false;

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
		private final long oid;
		//Do not store classes here! See above.
		//We also do not store the class name, as it uses a lot of space, especially since
		//we do not return pages to FSM except the last one.
		private String cName;   
		private int objIndexPage;
		private PagedPosIndex objIndex;
		private List<FieldIndex> fieldIndices = new LinkedList<FieldIndex>();
		private int schemaPage;
		private transient ZooClassDef classDef;
		
		/**
		 * Constructor for reading index.
		 */
		private SchemaIndexEntry(PageAccessFile raf) {
			oid = raf.readLong();
			objIndexPage = raf.readInt();
		    int nF = raf.readShort();
		    for (int i = 0; i < nF; i++) {
		    	FieldIndex fi = new FieldIndex();
		    	fieldIndices.add(fi);
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
		 * @param def 
		 * @throws IOException 
		 */
		private SchemaIndexEntry(String cName, PageAccessFile raf, long oid, ZooClassDef def) {
			this.oid = oid;
			this.cName = cName;
			this.objIndex = PagedPosIndex.newIndex(raf);
			this.classDef = def;
		}
		
		private void write(PageAccessFile raf) {
		    raf.writeLong(oid);
		    raf.writeInt(objIndexPage);  //no data page yet
		    raf.writeShort((short) fieldIndices.size());
		    for (FieldIndex fi: fieldIndices) {
		    	raf.writeString(fi.fName);
		    	raf.writeByte((byte) fi.fType.ordinal());
		    	raf.writeBoolean(fi.isUnique);
		    	raf.writeInt(fi.page);
		    }
		}

		public PagedPosIndex getObjectIndex() {
			// lazy loading
			if (objIndex == null) {
				objIndex = PagedPosIndex.loadIndex(raf, objIndexPage);
			}
			return objIndex;
		}

		public long getOID() {
			return oid;
		}
		
		public AbstractPagedIndex defineIndex(ZooFieldDef field, boolean isUnique) {
			//double check
			if (!field.isPrimitiveType() && !field.isString()) {
				throw new JDOUserException("Type can not be indexed: " + field.getTypeName());
			}
			for (FieldIndex fi: fieldIndices) {
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
				fi.index = new PagedUniqueLongLong(raf);
			} else {
				fi.index = new PagedLongLong(raf);
			}
			fieldIndices.add(fi);
			return fi.index;
		}

		public boolean removeIndex(ZooFieldDef field) {
			Iterator<FieldIndex> iter = fieldIndices.iterator();
			while (iter.hasNext()) {
				FieldIndex fi = iter.next(); 
				if (fi.fName.equals(field.getName())) {
					iter.remove();
					fi.index.clear();
					field.setIndexed(false);
					return true;
				}
			}
			return false;
		}

		public AbstractPagedIndex getIndex(ZooFieldDef field) {
			Iterator<FieldIndex> iter = fieldIndices.iterator();
			while (iter.hasNext()) {
				FieldIndex fi = iter.next(); 
				if (fi.fName.equals(field.getName())) {
					if (fi.index == null) {
						if (fi.isUnique) {
							fi.index = new PagedUniqueLongLong(raf, fi.page);
						} else {
							fi.index = new PagedLongLong(raf, fi.page);
						}
					}
					return fi.index;
				}
			}
			return null;
		}

		public List<AbstractPagedIndex> getIndices() {
			Iterator<FieldIndex> iter = fieldIndices.iterator();
			ArrayList<AbstractPagedIndex> indices = new ArrayList<AbstractPagedIndex>();
			while (iter.hasNext()) {
				FieldIndex fi = iter.next();
				indices.add(fi.index);
			}
			return indices;
		}

		public boolean isUnique(ZooFieldDef field) {
			Iterator<FieldIndex> iter = fieldIndices.iterator();
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
			Iterator<FieldIndex> iter = fieldIndices.iterator();
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
			cName = className;
		}

		public ZooClassDef getClassDef() {
			return classDef;
		}
	}

	public SchemaIndex(PageAccessFile raf, int indexPage1, boolean isNew) {
		this.isDirty = isNew;
		this.raf = raf;
		this.pageId = indexPage1;
		if (!isNew) {
			readIndex();
		}
	}
	
	private void readIndex() {
		raf.seekPageForRead(pageId, true);
		int nIndex = raf.readInt();
		for (int i = 0; i < nIndex; i++) {
			SchemaIndexEntry entry = new SchemaIndexEntry(raf);
			schemaIndex.put(entry.oid, entry);
		}
	}

	
	public int write() {
		//write the indices
		for (SchemaIndexEntry e: schemaIndex.values()) {
			if (e.objIndex != null) {
				int p = e.getObjectIndex().write();
				if (p != e.objIndexPage) {
					markDirty();
				}
				e.objIndexPage = p;
			}
			//write attr indices
			if (e.writeAttrIndices()) {
				markDirty();
			}
		}

		if (!isDirty()) {
			return pageId;
		}

		//now write the index directory
		//we can do this only afterwards, because we need to know the pages of the indices
		pageId = raf.allocateAndSeek(pageId, -1);

		//TODO we should use a PagedObjectAccess here. This means that we treat schemata as objects,
		//but would also allow proper use of FSM for them. 
		
		//number of indices
		raf.writeInt(schemaIndex.size());

		//write the index directory
		for (SchemaIndexEntry e: schemaIndex.values()) {
			e.write(raf);
		}

		markClean();

		return pageId;
	}

	private SchemaIndexEntry getSchema(String clsName) {
		//search schema in index
		for (SchemaIndexEntry e: schemaIndex.values()) {
			if (e.cName.equals(clsName)) {
				return e;
			}
		}
		return null;
	}
	
	public SchemaIndexEntry getSchema(long oid) {
		return schemaIndex.get(oid);
	}

	public Collection<SchemaIndexEntry> getSchemata() {
		return Collections.unmodifiableCollection(schemaIndex.values());
	}

    protected final boolean isDirty() {
        return isDirty;
    }
    
	protected final void markDirty() {
		isDirty = true;
	}
	
	protected final void markClean() {
		isDirty = false;
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
		raf.seekPage(fp.getPage(), fp.getOffs(), true);
		ZooClassDef def = Serializer.deSerializeSchema(raf);
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
	 * @return Null, if no matching schema could be found.
	 */
	public ZooClassDef readSchema(String clsName, PagedOidIndex oidIndex) {
		SchemaIndexEntry e = getSchema(clsName);
		if (e == null) {
			return null; //no matching schema found 
		}
		FilePos fp = oidIndex.findOid(e.getOID());
		raf.seekPage(fp.getPage(), fp.getOffs(), true);
		ZooClassDef def = Serializer.deSerializeSchema(raf);
		if (def.getSuperOID() != 0) {
			ZooClassDef defSuper = getSchema(def.getSuperOID()).classDef;
			def.associateSuperDef(defSuper);
		}
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
	

	public void refreshSchema(ZooClassDef def, PagedOidIndex oidIndex) {
		SchemaIndexEntry e = getSchema(def.getOid());
		if (e == null) {
			throw new JDOFatalDataStoreException(); 
		}
		FilePos fp = oidIndex.findOid(e.getOID());
		raf.seekPage(fp.getPage(), fp.getOffs(), true);
		Serializer.deSerializeSchema(raf, def);
		//def.associateSuperDef(defSuper);
		def.associateFields();
		//and check for indices
		//TODO maybe we do not need this for a refresh...
		for (ZooFieldDef f: def.getAllFields()) {
			if (e.getIndex(f) != null) {
				f.setIndexed(true);
				f.setUnique(e.isUnique(f));
			}
		}
	}

	
	/**
	 * @param oidIndex 
	 * @return List of all schemata in the database. These are loaded when the database is opened.
	 */
	public Collection<ZooClassDef> readSchemaAll(PagedOidIndex oidIndex) {
		Map<Long, ZooClassDef> ret = new HashMap<Long, ZooClassDef>();
		for (SchemaIndexEntry se: schemaIndex.values()) {
			FilePos fp = oidIndex.findOid(se.getOID());
			raf.seekPage(fp.getPage(), fp.getOffs(), true);
			ZooClassDef def = Serializer.deSerializeSchema(raf);
			ret.put( def.getOid(), def );
			se.setName( def.getClassName() );
			se.schemaPage = fp.getPage();
			se.classDef = def;
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

	public void defineSchema(ZooClassDef def) {
		String clsName = def.getClassName();
		long oid = def.getOid();
		
        // check if such an entry exists!
        if (getSchema(clsName) != null || getSchema(def.getOid()) != null) {
            throw new JDOFatalDataStoreException("Schema is already defined: " + clsName + 
                    " oid=" + Util.oidToString(oid));
        }
        SchemaIndexEntry entry = new SchemaIndexEntry(clsName, raf, oid, def);
        schemaIndex.put(oid, entry);
        markDirty();
	}

	public void writeSchema(ZooClassDef sch, boolean isNew, long oid, PagedOidIndex oidIndex) {
		SchemaIndexEntry theSchema = getSchema(sch.getOid());

		//allocate page
		//TODO write all schemata on one page (or series of pages)?
		//TODO reuse page?
		int prevPage = 0;
		if (!isNew) {
			prevPage = theSchema.schemaPage;
		}
		int schPage = raf.allocateAndSeek(prevPage, -1);
		Serializer.serializeSchema(sch, oid, raf);

		//Store OID in index
        int schOffs = 0;
		oidIndex.insertLong(oid, schPage, schOffs);
	}
	

	public void undefineSchema(ZooClassDef sch) {
		//We remove it from known schema list.
		SchemaIndexEntry entry = schemaIndex.remove(sch.getOid());
		markDirty();
		if (entry == null) {
			String cName = sch.getClassName();
			throw new JDOUserException("Schema not found: " + cName);
		}
		
		//field indices
		for (FieldIndex fi: entry.fieldIndices) {
			fi.index.clear();
		}
		
		//pos index
		entry.objIndex.clear();
	}	

	public void deleteSchema(ZooClassDef sch, PagedOidIndex oidIndex) {
		if (!sch.getSubClasses().isEmpty()) {
			//TODO first delete subclasses
			System.out.println("STUB delete subdata pages.");
		}
		
		//update OIDs
		oidIndex.removeOid(sch.getOid());
		
		//TODO remove from FSM

		//delete all associated data
//		int dataPage = entry._dataPage;
		System.out.println("STUB delete data pages, attribute indices and pos index.");
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

	public List<Integer> debugPageIdsAttrIdx() {
	    ArrayList<Integer> ret = new ArrayList<Integer>();
        for (SchemaIndexEntry e: schemaIndex.values()) {
            for (FieldIndex fi: e.fieldIndices) {
                ret.addAll(fi.index.debugPageIds());
            }
        }
        return ret;
	}

	public void renameSchema(ZooClassDef def, String newName) {
		//We remove it from known schema list.
		SchemaIndexEntry entry = getSchema(def.getOid());
		markDirty();
		if (entry == null) {
			String cName = def.getClassName();
			throw new JDOUserException("Schema not found: " + cName);
		}

		//Nothing to do, just rewrite it here.
		//TODO remove this method, should be automatically rewritten if ClassDef is dirty. 
	}
}
