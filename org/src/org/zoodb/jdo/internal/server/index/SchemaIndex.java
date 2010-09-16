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
		private final int _id;  //TODO remove ?!?! Use OIDs!!
		private final String _cName;  //Do not store classes here! See above. 
		private final int _objIndexPage;
		private final int _schemaPage;
		private final int _schemaPageOffset;
		private final PageAccessFile _raf;
		private final ObjectIndex _objIndex;
		
		private static int maxID = 0;  //TODO use OIDs!
		
		/**
		 * Constructor for reading index.
		 */
		public SchemaIndexEntry(int id, PageAccessFile raf) throws IOException {
			_id = id;
			if (id <= 0) throw new IllegalArgumentException("ID=" + id);
			if (id > maxID) {
				maxID = id;
			}
			_cName = raf.readString();
			_objIndexPage = raf.readInt();
			_schemaPage = raf.readInt();
			_schemaPageOffset = raf.readInt();
			_raf = raf;
			_objIndex = new ObjectIndex(_raf, _objIndexPage, false);
		}
		
		/**
		 * Constructor for creating new Index.
		 * @param id
		 * @param cName
		 * @param objIndexPage
		 * @param schPage
		 * @param schPageOfs
		 * @param raf
		 * @throws IOException 
		 */
		public SchemaIndexEntry(String cName, int objIndexPage, int schPage, 
				int schPageOfs, PageAccessFile raf) throws IOException {
			_id = ++maxID;
			_cName = cName;
			_objIndexPage = objIndexPage;
			_schemaPage = schPage;
			_schemaPageOffset = schPageOfs;
			_raf = raf;
			//create first page for obj index
			_raf.seekPage(_objIndexPage);
			_raf.writeLong(0);
			_raf.seekPage(_objIndexPage+1, -4);
			_raf.writeInt(0); 
			_objIndex = new ObjectIndex(raf, _objIndexPage, true);
		}
		
		private void write() {
			try {
				_raf.writeInt(_id);
				_raf.writeString(_cName);
				_raf.writeInt(_objIndexPage);  //no data page yet
				_raf.writeInt(_schemaPage);
				_raf.writeInt(_schemaPageOffset);
			} catch (IOException e) {
				throw new JDOFatalDataStoreException("Error writing schema index entry.", e);
			}
		}

		public ObjectIndex getObjectIndex() {
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
		super(raf, isNew);
		_indexPage1 = indexPage1;
		if (!isNew) {
			readIndex();
		} else {
			SchemaIndexEntry.maxID = 0;
		}
	}
	
	private void readIndex() {
		//TODO to improve paging behavior:
		//- either store page in SchemaIndexEntry, to later only store the entries of that page
		//- or use a bucket list with one bucker per page.

		int schNextPage = _indexPage1;
		try {
			//format: <ID> <schema> followed by <0>. <nextPage> is at the end of the page.
			while (schNextPage != 0) {
				_raf.seekPage(schNextPage);
				int id = _raf.readInt();
				while (id != 0) {
					SchemaIndexEntry entry = new SchemaIndexEntry(id, _raf);
					_schemaIndex.add(entry);
					id = _raf.readInt();
				}
				_raf.seekPage(schNextPage+1, -4); 
				schNextPage =_raf.readInt();
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error reading schema index.", e);
		}
	}

	
	public void write() {
		if (!isDirty()) {
			return;
		}
		//TODO to improve paging behavior:
		//- either store page in SchemaIndexEntry, to later only store the entries of that page
		//- or use a bucket list with one bucker per page.

		int nextPage = _indexPage1;
		
		try {
			Iterator<SchemaIndexEntry> iter = _schemaIndex.iterator();

			//loop for pages
			//start with do, because we need to write changes, even if the index is now empty
			do {
				_raf.seekPage(nextPage);
				
				//loop of index entries
				for (int i = 0; i < 5 && iter.hasNext(); i++) {  //TODO fix: use size instead of fixed count!!!
					SchemaIndexEntry e = iter.next();
					e.write();
				}
				_raf.checkOverflow(nextPage);
				
				//indicate end of entries on this page
				_raf.writeInt(0);
				
				_raf.seekPage(nextPage+1, -4);
				int currentPage = nextPage;
				nextPage =_raf.readInt();

				if (iter.hasNext() && nextPage == 0) {
					//allocate more pages
					nextPage = _raf.allocatePage();
					_raf.seekPage(currentPage+1, -4); 
					_raf.writeInt(nextPage);
					//write 0 to the end of the new page
					_raf.seekPage(nextPage+1, -4); 
					_raf.writeInt(0);
				}
			} while (iter.hasNext());
			//are there more pages? Is possible if we deleted schemas...
			if (nextPage != 0 && nextPage != _indexPage1) {
				System.out.println("FIXME: free unused pages.");  //TODO
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error reading schema index.", e);
		}
		markClean();
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
