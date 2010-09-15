package org.zoodb.jdo.internal.server.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.PageAccessFile;

/**
 * B-Tree like index structure.
 * There are two types of nodes, leaf nodes and inner nodes. Leaf nodes contain key-value pairs.
 * Inner nodes do not contain values. The contain n keys and (n||n+1) page references.
 * 
 * This paged OidIndex can be optimized towards the following properties:
 * - Unique entries
 * - (almost) ordered insertion.
 * - Hopefully (almost) consecutive insertion (unused values only after delete).
 * 
 * Deletion:
 * - Normal BTree deletion: 
 *   if (nEntry < min) then copy entries from prev/next pages
 *   -> if (nEntry < min then) two reads + two writes for every committed update
 *   -> pages are at least half filled -> Reasonable use of space
 *   Improvement: Distribute to prev&next page as soon as possible
 *   -> better use of space
 *   -> always 3 reads
 * - TZ deletion: 
 *   if (prev+this <= nEntries) then merge pages
 *   -> perfectly fine for leaf pages, could be improved to prev+this+next
 *   -> 2(3) reads, 1 writes (2 during merge).
 *   -> can lead to bad trees if used on inner pages and significant deletion in a deep tree;
 *      but still, badness is unlikely to be very bad, no unnecessary leafpages will ever be 
 *      created. TODO: check properly.
 *   -> Improvement: Store leaf size in inner page -> avoids reading neighbouring pages
 *      1 read per delete (1/2 during merge) But: inner pages get smaller. -> short values! 
 *      -> short values can be compressed (cutting of leading 0), because max value depends on
 *         page size, which is fixed. For OID pages: 64/1KB -> 6bit; 4KB->8bit; 16KB->10bit;  
 * - Naive deletion:
 *   Delete leaves only when page is empty
 *   -> Can completely prevent tree shrinkage.
 *   -> 1 read, 1 write
 *    
 * -> So far we go with the naive delete TODO!!!!!!
 * -> Always keep in mind: read access is most likely much more frequent than write access (insert,
 *    update/delete). 
 * -> Also keep in mind: especially inner index nodes are likely to be cached anyway, so they
 *    do not require a re-read. Caching is likely to occur until the index gets much bigger that
 *    memory.
 * -> To support previous statement, and in general: Make garbage collection of leaves easier than
 *    for inner nodes. E.g. reuse (gc-able) page buffers? TODO   
 * 
 * Pages as separate Objects vs hardlinked pages (current implementation).
 * Treating pages as objects is appealing, because most updates require only a single rewrite of the
 * local page and an update of the OID entry. But for OID-indices this is not advisable, because
 * any OID update triggers another OID update until an update fall on a page that is already dirty.
 * Another disadvantage is that values are 64bit, rather than 32bit page IDs. This could be helped
 * by always using page IDs and then again checking all objects on that page (reduced page-read
 * vs. increased CPU usage).  
 * 
 * @author Tilmann Zäschke
 *
 */
public class PagedOidIndex_OLD extends AbstractPagedIndex {

	private static final long MIN_OID = 100;
	
	public static class FilePos {
		final int page;
		final int offs;
		final long oid;
		FilePos(long oid, int page, int offs) {
			this.page = page;
			this.offs = offs;
			this.oid = oid;
		}
		FilePos(long oid, long pos) {
			this.oid = oid;
			this.page = (int)(pos >> 32);
			this.offs = (int)(pos & 0x00000000FFFFFFFF);
		}
		public int getPage() {
			return page;
		}
		public int getOffs() {
			return offs;
		}
		public long getOID() {
			return oid;
		}
		@Override
		public String toString() {
			return "FilePos::page=" + page + " ofs=" + offs + " oid=" + oid;
		}
	}
	
	private static class IteratorPos {
		IteratorPos(AbstractIndexPage page, short pos) {
			this.page = page;
			this.pos = pos;
		}
		//This is for the iterator, do _not_ use WeakRefs here.
		final AbstractIndexPage page;
		short pos;
	}

	class OidIterator implements Iterator<FilePos> {

		private OidIndexPage currentPage;
		private short currentPos = 0;
		private final long minKey;
		private final long maxKey;
		private final Stack<IteratorPos> stack = new Stack<IteratorPos>();
		
		public OidIterator(OidIndexPage root, long minKey, long maxKey) {
			//find page
			currentPage = root;
			navigateToNextLeaf();
			
			// now find pos in this page
			currentPos = 0;
			while (currentPage.keys[currentPos] < minKey) {
				currentPos++;
			}

			//next will increment this
			currentPos --;
			
			this.minKey = minKey;
			this.maxKey = maxKey;
		}

		@Override
		public boolean hasNext() {
			if (!currentPage.isLeaf) throw new IllegalStateException();  //TODO remove
			if (currentPage == null) {
				return false;
			}
			if (currentPos+1 < currentPage.nEntries) {
				if (currentPage.keys[currentPos+1] <= maxKey) {
					return true;
				}
				return false;
			}
			//currentPos >= nEntries -> no more on this page
			if (root == null) {
				return false;
			}
			
			//find next page
			IteratorPos ip = stack.pop();
			currentPage = (OidIndexPage) ip.page;
			currentPos = ip.pos;
			currentPos++;
			return navigateToNextLeaf();
		}

		private boolean navigateToNextLeaf() {
			while (currentPos > currentPage.nEntries) {
				if (stack.isEmpty()) {
					return false;
				}
				IteratorPos ip = stack.pop();
				currentPage = (OidIndexPage) ip.page;
				currentPos = ip.pos;
				currentPos++;
			}

			
			while (!currentPage.isLeaf) {
				//The stored value[i] is the min-values of the according page[i+1} 
				//TODO implement binary search
				for ( ; currentPos < currentPage.nEntries; currentPos++) {
					if (currentPage.keys[currentPos] > minKey) {
						//read page before that value
						break;
					}
				}
				//read last page
				stack.push(new IteratorPos(currentPage, currentPos));
				currentPage = currentPage.readOrCreatePage(currentPos);
				currentPos = 0;
			}
			//no need to check the pos, each leaf should have more than 0 entries;
			if (currentPage.keys[currentPos] <= maxKey) {
				currentPos--;
				return true;
			}
			return false;
		}
		
		@Override
		public FilePos next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			//hasNext should leave us on a leaf page
			currentPos++;
			return new FilePos(currentPage.keys[currentPos], currentPage.values[currentPos]);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	class OidIndexPage extends AbstractIndexPage {
		private final long[] keys;
		//TODO store only pages or also offs? -> test de-ser whole page vs de-ser single obj.
		//     -> especially, objects may not be valid anymore (deleted)! 
		private final long[] values;
		//transient final PageAccessFile paf;
		/** number of keys. There are nEntries+1 subPages in any leaf page. */
		short nEntries;
		
		public OidIndexPage(AbstractIndexPage root, boolean isLeaf) {
			super(root, isLeaf);
			if (isLeaf) {
				keys = new long[maxLeafN];
				values = new long[maxLeafN];
			} else {
				keys = new long[maxInnerN];
				values = null;
			}
		}

		//TODO change this.
		// each class should implement the read() method, and should call the super.readNonLeaf()
		// if necessary. This would avoid some casting. And the super class wouldn't have to call
		// it's own sub-class (code-smell!?).
		
		@Override
		void readData() {
			nEntries = paf.readShort();
			for (int i = 0; i < nEntries; i++) {
				keys[i] = paf.readLong();
				values[i] = paf.readLong();
			}
		}
		
		@Override
		void writeData() {
			paf.writeShort(nEntries);
			for (int i = 0; i < nEntries; i++) {
				paf.writeLong(keys[i]);
				paf.writeLong(values[i]);
			}
		}

		public OidIndexPage locatePageForKey(long key) {
			if (isLeaf) {
				return this;
			}
			//The stored value[i] is the min-values of the according page[i+1} 
			//TODO implement binary search
			for (short i = 0; i < nEntries; i++) {
				if (keys[i]>key) {
					//TODO use weak refs
					//read page before that value
					return readOrCreatePage(i).locatePageForKey(key);
				}
			}
			//read last page
			return readOrCreatePage(nEntries).locatePageForKey(key);
		}
		
		private OidIndexPage readOrCreatePage(short pos) {
			int pageId = leafPages[pos];
			OidIndexPage page = (OidIndexPage) leaves[pos];
			if (page != null) {
				//page is in memory
				//TODO take care it's not cached in ByteBuffer cache (double caching)
				//TODO take care it's not gc'd before a pageId is allocated
				return page;
			}
			
			//now try to load it
			if (pageId == 0) {
				//create new page
				page = createPage(this, true);
			} else {
				//load page
				page = (OidIndexPage) readPage(pageId, this);
			}
			leaves[pos] = page;
			return page;
		}

		public FilePos getValueFromLeaf(long oid) {
			if (!isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			//TODO use better search alg
			for (int i = 0; i < nEntries; i++ ) {
				if (keys[i] == oid) {
					return new FilePos( oid, values[i]);
				}
			}
			//TODO if non-unique, the value could be on the following page!
			return null;
		}

		public void put(long oid, int schPage, int schOffs) {
			if (!isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			if (nEntries < maxLeafN) {
				//add locally
				//TODO use better search alg
				for (int i = 0; i < nEntries; i++ ) {
					if (keys[i] == oid) {
						long newVal = (((long)schPage) << 32) | (long)schOffs;
						if (newVal != values[i]) {
							markDirty();
						}
						values[i] = newVal;
						return;
					} else if (keys[i] > oid) {
						System.arraycopy(keys, i, keys, i+1, nEntries-i);
						System.arraycopy(values, i, values, i+1, nEntries-i);
						keys[i] = oid;
						values[i] = (((long)schPage) << 32) | (long)schOffs;
						nEntries++;
						
						markPageDirty();
						return;
					}
				}
				//append entry
				keys[nEntries] = oid;
				values[nEntries] = (((long)schPage) << 32) | (long)schOffs;
				nEntries++;
				
				markPageDirty();
				return;
			} else {
				//treat page overflow
				OidIndexPage newP = new OidIndexPage(root, true);
				System.arraycopy(keys, minLeafN, newP.keys, 0, maxLeafN-minLeafN);
				System.arraycopy(values, minLeafN, newP.values, 0, maxLeafN-minLeafN);
				nEntries = (short) minLeafN;
				newP.nEntries = (short) (maxLeafN-minLeafN);
				//New page and min key
				root.addLeafPage(newP, newP.keys[0], this);
				markPageDirty();
				newP.markPageDirty();
				if (newP.keys[0] > oid) {
					put(oid, schPage, schOffs);
				} else {
					newP.put(oid, schPage, schOffs);
				}
			}
		}

		//TODO rename to addPage
		void addLeafPage(AbstractIndexPage newP, long minKey, AbstractIndexPage prevPage) {
			if (isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			if (nEntries < maxInnerN) {
				//add page here
				//TODO use better search alg (only possible when searching keys i.o. page.
				//     However, I'm searching for PageID here, because we want to insert the new
				//     page right after the previous one. In case they have all the same values,
				//     it would be hard to insert the new page in the right position.
				//TODO what is the right position? Does it matter? Insertion ordered?
				//TODO Isn't prevPage always the first page with the value? 
				//     (Currently yes, see search alg)
				//TODO Should we store non-unique values more efficiently? Instead of always storing
				//     the key as well? -> Additional page type for values only? The local value only
				//     being a reference to the value page (inlc offs)? How would efficient insertion
				//     work (shifting values and references to value pages???) ?
				
				
				
				
				//Important!
				//TODO if this is the OIDindex, then we can optimize page fill ratio by _not_ 
				//     copying half the data to the next page, but instead start a new empty page.
				//     Something similar (with value checking) could work for 'integer' indices, 
				//     (unique, or if all values are stored under the same key).
				//    -> For OID: additional bonus: The local leaf page needs no updates -> saves
				//    one page to write.
				//    -> Pages should not be completely filled in case of parallel transactions,
				//       OIDs may not be written in order! -> Alternative: control values! If 
				//       consecutive, no insertions will occur! ->  (last-first)=nMaxEntries
				
				//For now, we assume a unique index.
				//TODO more efficient search
				int i;
				for ( i = 0; i < nEntries; i++ ) {
					if (keys[i] > minKey) {
						break;
					}
				}
				System.arraycopy(keys, i, keys, i+1, nEntries-i);
				System.arraycopy(leaves, i+1, leaves, i+2, nEntries-i);
				System.arraycopy(leafPages, i+1, leafPages, i+2, nEntries-i);
				keys[i] = minKey;
				leaves[i+1] = newP;
				newP.root = this;
				leafPages[i+1] = 0;
				nEntries++;
				markPageDirty();
				return;
			} else {
				//treat page overflow
				OidIndexPage newInner = createPage(root, false);
				
				//TODO use optimized fill ration for OIDS, just like above.
				System.arraycopy(keys, minInnerN+1, newInner.keys, 0, nEntries-minInnerN-1);
				System.arraycopy(leaves, minInnerN+1, newInner.leaves, 0, nEntries-minInnerN);
				System.arraycopy(leafPages, minInnerN+1, newInner.leafPages, 0, nEntries-minInnerN);
				newInner.nEntries = (short) (nEntries-minInnerN-1);
				newInner.updateLeafRoot();

				if (root == null) {
					OidIndexPage newRoot = createPage(null, false);
					newRoot.leaves[0] = this;
					newRoot.leaves[1] = newInner;
					newRoot.keys[0] = keys[minInnerN];
					newRoot.nEntries = 1;
					this.root = newRoot;
					newInner.root = newRoot;
					updateRoot(newRoot);
				} else {
					root.addLeafPage(newInner, keys[minInnerN], this);
				}
				nEntries = (short) (minInnerN);
				markPageDirty();
				newInner.addLeafPage(newP, minKey, prevPage);
				return;
			}
		}
		
		public void print() {
			if (isLeaf) {
				System.out.println("Leaf page(): n=" + nEntries + " oids=" + Arrays.toString(keys));
			} else {
				System.out.println("Inner page(): n=" + nEntries + " oids=" + Arrays.toString(keys));
				System.out.println("                " + nEntries + " page=" + Arrays.toString(leafPages));
				for (int i = 0; i <= nEntries; i++) {
					if (leaves[i] != null) leaves[i].print();
					else System.out.println("Page not loaded.");
				}
			}
		}

		public void printLocal() {
			if (isLeaf) {
				System.out.println("Leaf page(): n=" + nEntries + " oids=" + Arrays.toString(keys));
			} else {
				System.out.println("Inner page(): n=" + nEntries + " oids=" + Arrays.toString(keys));
				System.out.println("                      " + Arrays.toString(leafPages));
			}
		}

		protected boolean remove(long oid) {
			for ( int i = 0; i < nEntries; i++ ) {
				if (keys[i] > oid) {
					return false;
				}
				if (keys[i] == oid) {
					// first remove the element
					System.arraycopy(keys, i+1, keys, i, nEntries-i-1);
					System.arraycopy(values, i+1, values, i, nEntries-i-1);
					nEntries--;
					markPageDirty();
//					if (nEntries < minLeafN) { //TODO
					if (nEntries == 0) {
						//TODO update higher level index entries with new min value (if oid==min)????
						//TODO
//						if (root != null) {
							nLeaves--;
							root.removeLeafPage(this);
//						}
					} else if (nEntries < (maxLeafN >> 1) && (nEntries % 8 == 0)) {
						//The second term prevents frequent reading of previous and following pages.
						
						//now attempt merging this page
						OidIndexPage prevPage = (OidIndexPage) root.getPrevLeafPage(this);
						if (prevPage != null) {
							//We merge only if they all fit on a single page. This means we may read
							//the previous page unnecessarily, but we avoid writing it as long as 
							//possible. TODO find a balance, and do no read prev page in all cases
							if (nEntries + prevPage.nEntries < maxLeafN) {
								//TODO for now this work only for leaves with the same root. We
								//would need to update the min values in the inner nodes.
								System.arraycopy(keys, 0, prevPage.keys, prevPage.nEntries, nEntries);
								System.arraycopy(values, 0, prevPage.values, prevPage.nEntries, nEntries);
								prevPage.nEntries += nEntries;
								prevPage.markPageDirty();
								nLeaves--;
								root.removeLeafPage(this);
							}
						}
					}
					return true;
				}
			}
			throw new JDOFatalDataStoreException();
		}

		@Override
		protected void removeLeafPage(AbstractIndexPage indexPage) {
			for (int i = 0; i <= nEntries; i++) {
				if (leaves[i] == indexPage) {
					if (nEntries > 0) { //otherwise we just delete this page
						if (i < nEntries) {  //otherwise it's the last element
							if (i > 0) {
								System.arraycopy(keys, i, keys, i-1, nEntries-i);
							} else {
								System.arraycopy(keys, 1, keys, 0, nEntries-1);
							}
							System.arraycopy(leaves, i+1, leaves, i, nEntries-i);
							System.arraycopy(leafPages, i+1, leafPages, i, nEntries-i);
						}
						nEntries--;
						markPageDirty();
						
						//Now try merging
						if (root == null) {
							return;
						}
						OidIndexPage prev = (OidIndexPage) root.getPrevLeafPage(this);
						if (prev == null) {
							return;
						}
						//TODO this is only good for merging inside the same root.
						if ((nEntries % 2 == 0) && (prev.nEntries + nEntries < maxInnerN)) {
							System.arraycopy(keys, 0, prev.keys, prev.nEntries+1, nEntries);
							System.arraycopy(leaves, 0, prev.leaves, prev.nEntries+1, nEntries+1);
							System.arraycopy(leafPages, 0, prev.leafPages, prev.nEntries+1, nEntries+1);
							//find key -> go up or go down????? Up!
							int pos = root.getPagePosition(this)-1;
							prev.keys[prev.nEntries] = ((OidIndexPage)root).keys[pos]; 
							prev.nEntries += nEntries + 1;  //for the additional key
							prev.updateLeafRoot();
							nInner--;
							root.removeLeafPage(this);
						}
					} else if (root != null) {
						nInner--;
						root.removeLeafPage(this);
					} else {
						//No root and this is a leaf page... -> we do nothing.
					}
					return;
				}
			}
			System.out.println("this:" + root);
			this.printLocal();
			System.out.println("leaf:");
			indexPage.printLocal();
			throw new JDOFatalDataStoreException("leaf page not found.");
		}

		public long getMax() {
			if (isLeaf) {
				if (nEntries == 0) {
					return MIN_OID;
				}
				return keys[nEntries-1];
			}
			//handle empty indices
			if (nEntries == 0 && leaves[nEntries] == null && leafPages[nEntries] == 0) {
				return MIN_OID;
			}
			long max = ((OidIndexPage)getPageByPos(nEntries)).getMax();
			return max;
		}

		@Override
		void writeKeys() {
			_raf.writeShort(nEntries);
			for (long l: keys) {
				_raf.writeLong(l);
			}
		}

		@Override
		void readKeys() {
			nEntries = _raf.readShort();
			for (int i = 0; i < keys.length; i++) {
				keys[i] = _raf.readLong();
			}
		}
	}
	
	private transient long _lastAllocatedInMemory = 100;
	private transient OidIndexPage root;
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public PagedOidIndex_OLD(PageAccessFile raf) {
		super(raf, true, 8, 8);
		System.out.println("OidIndex entries per page: " + maxLeafN + " / inner: " + maxInnerN);
		//bootstrap index
		root = createPage(null, false);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedOidIndex(PageAccessFile raf, int pageId) {
		super(raf, true, 8, 8);
		root = (OidIndexPage) readRoot(pageId);
		_lastAllocatedInMemory = root.getMax();
	}

	public void addOid(long oid, int schPage, int schOffs) {
		OidIndexPage page = getRoot().locatePageForKey(oid);
		page.put(oid, schPage, schOffs);
	}

	public boolean removeOid(long oid) {
		OidIndexPage page = getRoot().locatePageForKey(oid);
		return page.remove(oid);
	}

	public FilePos findOid(long oid) {
		OidIndexPage page = getRoot().locatePageForKey(oid);
		return page.getValueFromLeaf(oid);
	}

	public long[] allocateOids(int oidAllocSize) {
		long l1 = _lastAllocatedInMemory;
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

	@Override
	OidIndexPage createPage(AbstractIndexPage parent, boolean isLeaf) {
		return new OidIndexPage(parent, isLeaf);
	}

	@Override
	protected OidIndexPage getRoot() {
		return root;
	}

	public Iterator<FilePos> iterator() {
		return new OidIterator(getRoot(), 0, Long.MAX_VALUE);
	}

	@Override
	protected void updateRoot(AbstractIndexPage newRoot) {
		root = (OidIndexPage) newRoot;
	}
	
	public void print() {
		root.print();
	}

	public long getMaxValue() {
		return root.getMax();
	}
}
