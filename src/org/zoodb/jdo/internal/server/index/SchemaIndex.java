package org.zoodb.jdo.internal.server.index;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.PageAccessFile;

public class SchemaIndex extends AbstractIndex {

	private final List<SchemaIndexEntry> _schemaIndex = new LinkedList<SchemaIndexEntry>();
	private int _indexPage1 = -1;

	
	/**
	 * Do not store classes here. On the server, the class may not be available.
	 * 
	 * Otherwise it would be nice, because comparison of references to classes is faster than
	 * Strings, and reference require much less space. Then again, there are few schema classes,
	 * so space is not an argument. 
	 *
	 */
	public static class SchemaIndexEntry {
		private final PageAccessFile _raf;
		private final int _id;  //TODO remove ?!?! Use OIDs!!
		private final String _cName;  //Do not store classes here! See above. 
		private final int _schemaPage;
		private final int _schemaPageOffset;
		private int _objIndexPage;
		private PagedPosIndex _objIndex;
		
		private static int maxID = 0;  //TODO use OIDs!
		
		/**
		 * Constructor for reading index.
		 */
		public SchemaIndexEntry(PageAccessFile raf) {
			_raf = raf;
			_id = raf.readInt();
			if (_id <= 0) throw new IllegalArgumentException("ID=" + _id);
			if (_id > maxID) {
				maxID = _id;
			}
			_cName = raf.readString();
			_objIndexPage = raf.readInt();
			_schemaPage = raf.readInt();
			_schemaPageOffset = raf.readInt();
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
				int schPageOfs, PageAccessFile raf) {
			_raf = raf;
			_id = ++maxID;
			_cName = cName;
			_schemaPage = schPage;
			_schemaPageOffset = schPageOfs;
			_objIndex = PagedPosIndex.newIndex(raf);
		}
		
		public static SchemaIndexEntry read(PageAccessFile raf) {
			return new SchemaIndexEntry(raf);
		}

		private void write(PageAccessFile raf) {
		    raf.writeInt(_id);
		    raf.writeString(_cName);
		    raf.writeInt(_objIndexPage);  //no data page yet
		    raf.writeInt(_schemaPage);
		    raf.writeInt(_schemaPageOffset);
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
	}

	public SchemaIndex(PageAccessFile raf, int indexPage1, boolean isNew) {
		super(raf, isNew, true);
		_indexPage1 = indexPage1;
		if (!isNew) {
			readIndex();
		} else {
			SchemaIndexEntry.maxID = 0;
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
}
