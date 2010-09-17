package org.zoodb.jdo.internal.server.index;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.jdo.internal.server.PageAccessFile;

abstract class AbstractPagedIndex extends AbstractIndex {

	abstract class AbstractPageIterator<E> implements Iterator<E> {
		//TODO use different map to accommodate large numbers of pages?
		//We only have a map with original<->clone associations.
		//There can only be a need to clone a page if it has been modified. If it has been modified,
		//it must have been loaded and should be in the list of loaded leaf-pages.
		private final Map<AbstractIndexPage, AbstractIndexPage> pageClones = 
			new HashMap<AbstractIndexPage, AbstractIndexPage>();
		
		protected AbstractPageIterator() {
			//nothing
		}
		
		/**
		 * This method checks whether this iterator is interested in the given page, and whether
		 * the iterator already has a copy of that page.
		 * If the page is cloned, then the clone is returned for further us by other iterators.
		 * Sharing is possible, because iterators can not modify pages.
		 * @param page
		 * @param clone or null
		 * @param modcount
		 * @return null or the clone.
		 */
		AbstractIndexPage pageUpdateNotify(AbstractIndexPage page, AbstractIndexPage clone, 
				int modcount) {
			//TODO pageIDs for not-yet committed pages??? -> use original pages as key???
			if (!pageClones.containsKey(page) && pageIsRelevant(page)) {
				if (clone == null) {
					clone = page.clone();
				}
				pageClones.put(page, clone);
				//maybe we are using it right now?
				replaceCurrentIfEqual(page, clone);
			}
			//this can still be null
			return clone;
		}

		abstract void replaceCurrentIfEqual(AbstractIndexPage equal, AbstractIndexPage replace);
		
		protected final void releasePage(AbstractIndexPage oldPage) {
			//just try it, even if it is not in the list.
			pageClones.remove(oldPage);
		}
		
		protected final AbstractIndexPage findPage(AbstractIndexPage currentPage, short pos) {
			return currentPage.readOrCreatePage(pos, pageClones);
		}

		/**
		 * Check whether the keys on the page overlap with the min/max/current values of this 
		 * iterator.
		 */
		abstract boolean pageIsRelevant(AbstractIndexPage page);
		
	}
	
	
	//TODO is there a disadvantage in using non-static inner classes????
	protected abstract class AbstractIndexPage implements Cloneable {

		transient boolean isDirty;
		final transient boolean isLeaf;
		AbstractIndexPage root;
		final AbstractIndexPage[] leaves;
		final int[] leafPages;
		private int pageId = -1;
		
		//This map contains pages when they are loaded from disk.
		//It is used by iterators (and the index itself) to avoid double-loading pages used in
		//the index or other iterators.
		//Pages should be removed from this map when they get dirty, because the may get modified
		//multiple times, so iterators can not rely on getting the correct version, and the page
		//may be modified while the iterator is using it. -> Remove once they get dirty! TODO
		private transient Map<Integer, WeakReference<AbstractIndexPage>> pageCache = 
			new HashMap<Integer, WeakReference<AbstractIndexPage>>();
		
		AbstractIndexPage(AbstractIndexPage root, boolean isLeaf) {
			this.root = root;
			if (!isLeaf) {	
				leaves = new AbstractIndexPage[maxInnerN + 1];
				leafPages = new int[maxInnerN + 1];
				nInner++;
			} else {
				leaves = null;
				leafPages = null;
				nLeaves++;
			}
			this.isLeaf = isLeaf;

			//new pages are always dirty
			markPageDirty();
		}

		@Override
		public AbstractIndexPage clone() {
			AbstractIndexPage page = null;
			try {
				page = (AbstractIndexPage) super.clone();
			} catch (CloneNotSupportedException e) {
				throw new JDOFatalDataStoreException("Clone failed", e);
			}
			page.isDirty = isDirty;
			//page.isLeaf = isLeaf;
			System.arraycopy(leafPages, 0, page.leafPages, 0, leafPages.length);
			System.arraycopy(leaves, 0, page.leaves, 0, leaves.length);
			page.pageId = pageId;
			page.root = root;
			return page;
		}

		protected final void markPageDirty() {
			if (pageCache.remove(this.pageId) != null) {
				//The condition is arbitrary, we just use it to avoid calling this too often:
				System.out.println("Cleaning up pages");  //TODO
				for (Map.Entry<Integer, WeakReference<AbstractIndexPage>> e: pageCache.entrySet()) {
					if (e.getValue().get() == null) {
						pageCache.remove(e.getKey());
					}
				}
			}
			
			//always do this, even if page is already dirty:
			//create clone
			//TODO what about parents? Make them dirty again?
			if (!iterators.isEmpty()) {
				Iterator<AbstractPageIterator<?>> iterIter = iterators.keySet().iterator();
				AbstractIndexPage clone = null;
				while (iterIter.hasNext()) {
					AbstractPageIterator<?> indexIter = iterIter.next();
					clone = indexIter.pageUpdateNotify(this, clone, modcount);
				}
			}
			
			
			if (!isDirty) {
				isDirty = true;
				if (root != null) {
					root.markPageDirty();
				} else {
					//this is root, mark the wrapper dirty.
					markDirty();
				}
			}
		}

		
		protected final AbstractIndexPage readOrCreatePage(short pos) {
			int pageId = leafPages[pos];
			AbstractIndexPage page = leaves[pos];
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
			} else if (pageCache.containsKey(pageId) && 
			        (page = pageCache.get(pageId).get()) != null) {
				//Check after assignment to avoid race condition.
			} else {
				//load page
				page = readPage(pageId, this);
				pageCache.put(pageId, new WeakReference<AbstractIndexPage>(page));
			}
			leaves[pos] = page;
			return page;
		}
		
		
		protected final AbstractIndexPage readOrCreatePage(short pos, 
				Map<AbstractIndexPage, AbstractIndexPage> transientClones) {
			int pageId = leafPages[pos];
			AbstractIndexPage page = leaves[pos];
			if (page != null) {
				//page is in memory
				//TODO take care it's not cached in ByteBuffer cache (double caching)
				//TODO take care it's not gc'd before a pageId is allocated
				
				//Is there is a transient clone?
				if (transientClones.containsKey(page)) {
					return transientClones.get(page);
				}
				//okay there is no clone, use the original.
				return page;
			}
			
			//now try to load it
			if (pageId == 0) {
				//create new page
				page = createPage(this, true);
			} else if ((page = pageCache.get(pageId).get()) != null) {
				//Check after assignment to avoid race condition.
			} else {
				//load page
				page = readPage(pageId, this);
				pageCache.put(pageId, new WeakReference<AbstractIndexPage>(page));
			}
			leaves[pos] = page;
			return page;
		}
		
		
		final int write() {
			if (!isDirty) {
				return pageId;
			}

			if (isLeaf) {
				pageId = paf.allocateAndSeek();
				paf.writeShort((short) 0);
				writeData();
			} else {
				//first write the sub pages, because they will update the page index.
				for (int i = 0; i < leaves.length; i++) {
					AbstractIndexPage p = leaves[i];
					if (p == null) {
						continue;
					}
					leafPages[i] = p.write();
				}
				//TODO optimize: write only non-empty entries
				//TODO optimize: find a way to first write the inner nodes, then the leaves. Could
				//     be faster when reading the index. -> SDD has no such problem !!?!??
				
				//now write the page index
				pageId = paf.allocateAndSeek();
				paf.writeShort((short) leaves.length);
				for (int page: leafPages) {
					//TODO read only as long as page != 0, then jump to leaves.length;
					paf.writeInt(page);
				}
				writeKeys();
			}
			isDirty = false;
			return pageId;
		}


		abstract void writeKeys();

		abstract void writeData();

		abstract void readData();

		abstract void addLeafPage(AbstractIndexPage newPage, long minValue, AbstractIndexPage prevPage);

		public abstract void print();

		protected abstract void removeLeafPage(AbstractIndexPage indexPage);

		/**
		 * TODO for now this ignores leafPages on a previous inner node. It returns only leaf pages
		 * from the current node.
		 * @param indexPage
		 * @return The previous leaf page or null, if the given page is the first page.
		 */
		protected AbstractIndexPage getPrevLeafPage(AbstractIndexPage indexPage) {
			int pos = getPagePosition(indexPage);
			if (pos > 0) {
				return getPageByPos(pos-1);
			}
			if (root == null) {
				return null;
			}
			//TODO we really should return the last leaf page of the previous inner page.
			//But if they get merged, then we have to shift minimal values, which is
			//complicated. For now we ignore this case, hoping that it doesn't matter much.
			return null;
		}

		/**
		 * Returns (and loads, if necessary) the page at the specified position.
		 */
		protected AbstractIndexPage getPageByPos(int pos) {
			AbstractIndexPage page = leaves[pos];
			if (page != null) {
				return page;
			}
			page = readPage(leafPages[pos], this);
			leaves[pos] = page;
			return page;
		}

		/**
		 * This method will fail if called on the first page in the tree. However this should not
		 * happen, because when called, we already have a reference to a previous page.
		 * @param oidIndexPage
		 * @return The position of the given page with 0 <= pos < nEntries.
		 */
		int getPagePosition(AbstractIndexPage indexPage) {
			//We know that the element exists, so we iterate to list.length instead of nEntires 
			//(which is not available in this context at the moment.
			for (int i = 0; i < leaves.length; i++) {
				if (leaves[i] == indexPage) {
					return i;
				}
			}
			throw new JDOFatalDataStoreException("Leaf page not found in parent page.");
		}

		public abstract void printLocal();
		
		protected void updateLeafRoot() {
			for (AbstractIndexPage leaf: leaves) {
				if (leaf == null) {
					break;
				}
				leaf.root = this;
			}
		}

		abstract void readKeys();
	}

	protected transient final int maxLeafN;
	//Max number of keys in inner page (there can be max+1 page-refs)
	protected transient final int maxInnerN;
	//TODO if we ensure that maxXXX is a multiple of 2, then we could scrap the minXXX values
	protected transient final int minLeafN;
	protected transient final int minInnerN;
	protected final PageAccessFile paf;
	protected int nLeaves = 0;
	protected int nInner = 0;
	
	
	//COW stuff
	//TODO make concurrent?!?
	private final WeakHashMap<AbstractPageIterator<?>, Object> iterators = 
		new WeakHashMap<AbstractPageIterator<?>, Object>(); 
	private int modcount = 0;
	

	/**
	 * In case this is an existing index, read() should be called afterwards.
	 * Key and value length are used to calculate the man number of entries on a page.
	 * 
	 * @param raf The read/write byte stream.
	 * @param isNew Whether this is a new index or existing (i.e. read from disk).
	 * @param keyLen The number of bytes required for the key.
	 * @param valLen The number of bytes required for the value.
	 */
	public AbstractPagedIndex(PageAccessFile raf, boolean isNew, int keyLen, int valLen) {
		super(raf, isNew);
		
		paf = raf;
		
		//how many entries fit on one page?
		//
		//all pages: 
		//- 0 byte for flags (isRoot=(root==null)), isLeaf(leaves==0), isDirty(is transient))
		//- // NO! root page: 4 byte  -> Don't store the root! We would have to update it!!!
		//- # leaves: 2 byte
		//- # entries: 2 byte
		//- TODO index-ID (oid)?
		//- TODO index-type (oid for the schema of an index)????
		//
		//middle pages: 
		//- keyLen = keyLen ; refLen = 4byte (pageID)
		//- n values; n+1 references
		//---> n = (PAGE_SIZE - 4 - refLen) / (keyLen + refLen)
		//  
		//leave pages: keyLen = keyLen ; valLen = valLen
		//- n values
		//---> n = (PAGE_SIZE - 4) / (keyLen + valLen)

		final int pageHeader = 4; // 2 + 2
		final int refLen = 4;  //one int for pageID
		// we use only int, so it should round down automatically...
		maxLeafN = (DiskAccessOneFile.PAGE_SIZE - pageHeader) / (keyLen + valLen);
		if (maxLeafN * (keyLen + valLen) + pageHeader > DiskAccessOneFile.PAGE_SIZE) {
			throw new JDOFatalDataStoreException("Illegal Index size: " + maxLeafN);
		}
		minLeafN = maxLeafN >> 1;
		
		//-2 for short nKeys
		maxInnerN = (DiskAccessOneFile.PAGE_SIZE - pageHeader - refLen - 2) / (keyLen + refLen);
		if (maxInnerN * (keyLen + refLen) + pageHeader + refLen > DiskAccessOneFile.PAGE_SIZE) {
			throw new JDOFatalDataStoreException("Illegal Index size: " + maxInnerN);
		}
		minInnerN = maxInnerN >> 1;
	}

	abstract AbstractIndexPage createPage(AbstractIndexPage parent, boolean isLeaf);

	public final int write() {
		if (!getRoot().isDirty) {
			return getRoot().pageId;
		}
		
		return getRoot().write();
	}

	protected final AbstractIndexPage readRoot(int pageId) {
		return readPage(pageId, null);
	}
	
	protected abstract AbstractIndexPage getRoot();
	
	final AbstractIndexPage readPage(int pageId, AbstractIndexPage parentPage) {
		if (pageId == 0) {
			throw new IllegalArgumentException();
		}
		//TODO improve compression:
		//no need to store number of entries in leaf pages? Max number is given in code, 
		//actual number is where pageID!=0.
		paf.seekPage(pageId);
		int nL = paf.readShort();
		AbstractIndexPage newPage;
		if (nL == 0) {
			newPage = createPage(parentPage, true);
			newPage.readData();
		} else {
			newPage = createPage(parentPage, false);
//			leafPages = new int[maxLeafN];
			for (int i = 0; i < nL; i++) {
				newPage.leafPages[i] = paf.readInt();
				//TODO
//				We need to set the nEntries for the inner page!
//				Also: where do we read the keys for the inner page????
//				And: Best split into AbstractLeaf and AbstractInner.
//				Or: Just have leaf and inner and implement delegates for the key handling.
			}
			newPage.readKeys();
		}
		newPage.pageId = pageId;  //TODO check what this is good for...
		return newPage;
	}

	protected abstract void updateRoot(AbstractIndexPage newRoot);

	public int statsGetInnerN() {
		return nInner;
	}

	public int statsGetLeavesN() {
		return nLeaves;
	}
	
	public AbstractPageIterator<?> registerIterator(AbstractPageIterator iter) {
		iterators.put(iter, null);
		return iter;
	}
	
	public void deregisterIterator(AbstractPageIterator iter) {
		iterators.remove(iter);
	}
}
