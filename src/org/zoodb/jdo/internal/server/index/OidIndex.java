package org.zoodb.jdo.internal.server.index;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.Util;
import org.zoodb.jdo.internal.server.PageAccessFile;

public class OidIndex extends AbstractIndex {

	private transient long _lastAllocatedInMemory = 100;
	
	private SortedMap<Long,OidIndexEntry> _oids = new TreeMap<Long,OidIndexEntry>();
	
	static class OidIndexEntry {
		private final long _oid;  //TODO remove ?!?!
		private final int _dataPage;
		private final int _dataPageOffset;
		
		public OidIndexEntry(long oid, PageAccessFile raf) throws IOException {
			_oid = oid;
			if (oid <= 0) throw new IllegalArgumentException("OID=" + oid);  
			_dataPage = raf.readInt();
			_dataPageOffset = raf.readInt();
		}
		
		public OidIndexEntry(long oid, int dataPage, int dataPageOfs) {
			_oid = oid;
			if (oid <= 0) throw new IllegalArgumentException("OID=" + oid);  
			_dataPage = dataPage;
			_dataPageOffset = dataPageOfs;
		}
		
		void write(PageAccessFile raf) throws IOException {
			raf.writeLong(_oid);
			raf.writeInt(_dataPage);  //no data page yet
			raf.writeInt(_dataPageOffset);
		}

		public int getPage() {
			return _dataPage;
		}

		public int getOfs() {
			return _dataPageOffset;
		}
	}
	
	private int _oidPage1 = -1;
	private final PageAccessFile _raf;
	
	public OidIndex(PageAccessFile raf, int oidPage) throws IOException {
		super(raf, false);
		_raf = raf;
		_oidPage1 = oidPage;
		readIndex();
	}

	long[] allocateOids(int oidAllocSize) {
		long l1 = _lastAllocatedInMemory;  //TODO get(size) may not be very fast...
		long l2 = l1 + oidAllocSize;

		long[] ret = new long[(int) (l2-l1)];
		for (int i = 0; i < l2-l1; i++ ) {
			ret[i] = l1 + i + 1;
		}
		
		_lastAllocatedInMemory += oidAllocSize;
		if (_lastAllocatedInMemory < 0) {
			throw new JDOFatalDataStoreException("OID overflow after alloc: " + oidAllocSize);
		}
		//do not set dirty here!
		return ret;
	}

	public boolean removeOid(long oid) {
		OidIndexEntry e = _oids.remove(oid);
		if (e != null) {
			markDirty();
		} else {
			new IllegalArgumentException("oid=" + Util.oidToString(oid)).printStackTrace(); //TODO keep this?
		}
		return e != null;
	}

	private void readIndex() throws IOException {
		int nextPage = _oidPage1;
		long maxOid = 0;
		while (nextPage != 0) {
			_raf.seekPage(nextPage, false);
			
			long oid = _raf.readLong();
			while(oid != 0) {
				_oids.put(oid, new OidIndexEntry(oid, _raf));
				if (maxOid < oid) {
					maxOid = oid;
				}
				oid = _raf.readLong();
			}
			//following page, 0 for last page
			_raf.seekPage(nextPage+1, -4, false);
			nextPage =_raf.readInt(); 
		}
		_lastAllocatedInMemory = maxOid > 100 ? maxOid : 100;
	}

	public OidIndexEntry addOid(long oid, int dataPage, int dataOffs) {
		if (_oids.containsKey(oid)) {
			System.out.println("WARNING Overwriting OID: " + Util.oidToString(oid));
		}
		
		OidIndexEntry oie = new OidIndexEntry(oid, dataPage, dataOffs); 
		_oids.put(oid, oie);
		markDirty();
		return oie;
	}

	public void write() {
		if (!isDirty()) {
			return;
		}
		
		//TODO to improve paging behavior:
		//- either store page in SchemaIndexEntry, to later only store the entries of that page
		//- or use a bucket list with one bucker per page.

		int nextPage = _oidPage1;
		
		try {
			Iterator<OidIndexEntry> iter = _oids.values().iterator();

			//loop for pages
			//start with do, because we need to write changes, even if the index is now empty
			do {
				_raf.seekPage(nextPage, false);

				//loop of index entries
				for (int i = 0; i < 50 && iter.hasNext(); i++) {  //TODO fix: use size instead of fixed count!!!
					OidIndexEntry e = iter.next();
					e.write(_raf);
				}
				_raf.checkOverflow(nextPage);

				//indicate no more OIDS, e/g/ after oids were deleted.
				_raf.writeLong(0);
				
				_raf.seekPage(nextPage+1, -4, false);
				int currentPage = nextPage;
				nextPage =_raf.readInt();

				//allocate more pages?
				if (iter.hasNext() && nextPage==0) {
					nextPage = _raf.allocatePage(false);
					_raf.seekPage(currentPage+1, -4, false); 
					_raf.writeInt(nextPage);
					//write 0 to the end of the new page
					_raf.seekPage(nextPage+1, -4, false); 
					_raf.writeInt(0);
				}
			} while (iter.hasNext());
			//are there more pages? Is possible if we deleted schemas...
			if (nextPage != 0 && nextPage != _oidPage1) {
				System.out.println("FIXME: free unused OID pages.");  //TODO
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error reading schema index.", e);
		}
		
		markClean();
	}

	public OidIndexEntry findOid(long oid) {
		return _oids.get(oid);
	}

	public void createOrUpdate(long oid, int page, int offset) {
//		OidIndexEntry e = _oids.get(oid);
//		if (e == null) {
		//Simply overwrite the existing entry!
		OidIndexEntry oie = new OidIndexEntry(oid, page, offset); 
		_oids.put(oid, oie);
		markDirty();
//		}
//		e._dataPage = page;
//		e._dataPageOffset = offset;
	}
}
