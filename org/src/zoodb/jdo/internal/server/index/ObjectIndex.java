package org.zoodb.jdo.internal.server.index;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.Util;
import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.jdo.internal.server.PageAccessFile;

/**
 * This is a general index to be used by types, for example one for all schema instances, one for
 * all objects of each class, ... .
 * 
 * @author Tilmann Zäschke
 *
 */
public class ObjectIndex extends AbstractIndex {

	//TODO use specialised verion that stores long i.o Long, -> saves memory and is faster.
	//TODO in case we decide to use Long here, we could also use proper index entries possibly with
	//transient references to the OidEntry.
	
	//not sorted, but insertion ordered. (avoids updating more than one page if an entry is added,
	//i.e. avoids rehashing).
	private LinkedHashSet<Long> _oids = new LinkedHashSet<Long>();
	
	private int _oidPage1 = -1;
	private final PageAccessFile _raf;
	private boolean _isHollow = true;
	
	/**
	 * Constructor for reading index from disk.
	 * @param raf
	 * @param oidPage
	 * @param name
	 * @throws IOException
	 */
	public ObjectIndex(PageAccessFile raf, int oidPage, boolean isNew) throws IOException {
		super(raf, isNew);
		_raf = raf;
		_oidPage1 = oidPage;
		_isHollow = !isNew;
	}

	public void removeOid(long oid) {
		readIfHollow();
		if (_oids.remove(oid)) {
			markDirty();
		} else {
			new IllegalArgumentException("oid=" + Util.oidToString(oid)).printStackTrace(); //TODO keep this?
		}
	}

	private void readIndex() throws IOException {
		if (!_isHollow) {
			throw new IllegalStateException();
		}
		int nextPage = _oidPage1;
		while (nextPage != 0) {
			_raf.seekPage(nextPage);
			
			long oid = _raf.readLong();
			while(oid != 0) {
				_oids.add(oid);
				oid = _raf.readLong();
			}
			//following page, 0 for last page
			_raf.seekPage(nextPage+1, -4);
			nextPage =_raf.readInt(); 
		}
		_isHollow = false;
	}

	public void addOid(long oid) {
		readIfHollow();
		if (_oids.contains(oid)) {
			System.out.println("WARNING Overwriting OID: " + Util.oidToString(oid));
		}
		_oids.add(oid);
		markDirty();
	}

	public void write() {
		if (!isDirty()) {
			return;
		}
		if (_isHollow) {
			throw new IllegalStateException();
		}
		
		//TODO to improve paging behavior:
		//- either store page in SchemaIndexEntry, to later only store the entries of that page
		//- or use a bucket list with one bucker per page.

		int nextPage = _oidPage1;
		int currentPage = -1;
		
		try {
			Iterator<Long> iter = _oids.iterator();

			//loop for pages
			//start with do, because we need to write changes, even if the index is now empty
			do {
				_raf.seekPage(nextPage);

				//loop of index entries
				int pos = 0;
				//leave space for 0-marker and for next-page-field
				while (pos < DiskAccessOneFile.PAGE_SIZE-16 && iter.hasNext()) {
					_raf.writeLong(iter.next());
					pos+=8;
				}
				//indicate no more OIDS, e/g/ after oids were deleted.
				_raf.writeLong(0);

				_raf.checkOverflow(nextPage);
				
				_raf.seekPage(nextPage+1, -4);
				currentPage = nextPage;
				nextPage = _raf.readInt();

				//allocate more pages?
				if (iter.hasNext() && nextPage==0) {
					nextPage = _raf.allocatePage();
					_raf.seekPage(currentPage+1, -4); 
					_raf.writeInt(nextPage);
					//write 0 to the end of the new page
					_raf.seekPage(nextPage+1, -4); 
					_raf.writeInt(0);
				}
			} while (iter.hasNext());
			//are there more pages? Is possible if we deleted schemas...
			if (nextPage != 0 && nextPage != _oidPage1) {
				System.out.println("FIXME: free unused OID pages.");  //TODO
				_raf.seekPage(currentPage+1, -4); 
				_raf.writeInt(0);
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error reading index.", e);
		}

		markClean();
	}

	private void readIfHollow() {
		try {
			if (_isHollow) {
				if (isDirty()) {
					throw new IllegalStateException();
				}
				readIndex();
				_isHollow = false;
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error reading ObjIndex.", e);
		}
	}

	public Iterator<Long> objIterator() {
		readIfHollow();
		return _oids.iterator();
	}
}
