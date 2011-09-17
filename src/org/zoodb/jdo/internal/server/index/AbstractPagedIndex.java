package org.zoodb.jdo.internal.server.index;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.WeakHashMap;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.stuff.CloseableIterator;

/**
 * @author Tilmann Zäschke
 */
public abstract class AbstractPagedIndex extends AbstractIndex {

	public interface LongLongIndex {
		void insertLong(long key, long value);

		AbstractPageIterator<LLEntry> iterator(long minValue, long maxValue);

		long removeLong(long key, long value);
	}
	
	public abstract static class AbstractPageIterator<E> implements CloseableIterator<E> {
		private final AbstractPagedIndex ind;
		//TODO use different map to accommodate large numbers of pages?
		//We only have a map with original<->clone associations.
		//There can only be a need to clone a page if it has been modified. If it has been modified,
		//it must have been loaded and should be in the list of loaded leaf-pages.
		private final Map<AbstractIndexPage, AbstractIndexPage> pageClones = 
			new IdentityHashMap<AbstractIndexPage, AbstractIndexPage>();
		
		protected AbstractPageIterator(AbstractPagedIndex ind) {
			this.ind = ind;
			//we have to register the iterator now, because the sub-constructors may already
			//de-register it.
			this.ind.registerIterator(this);
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
					clone = page.newInstance();
					//currently, iterators do not need the parent field. No need to clone it then!
					clone.setParent(null); //pageClones.get(page.parent);
				}
				pageClones.put(page, clone);
				clone.setOriginal( page );
				//maybe we are using it right now?
				replaceCurrentAndStackIfEqual(page, clone);
			}
			//this can still be null
			return clone;
		}

		abstract void replaceCurrentAndStackIfEqual(AbstractIndexPage equal, AbstractIndexPage replace);
		
		protected final void releasePage(AbstractIndexPage oldPage) {
			//just try it, even if it is not in the list.
			if (pageClones.remove(oldPage.getOriginal()) == null && oldPage.getOriginal() != null) {
			    System.out.println("Cloned page not found!");
			}
		}
		
		protected final AbstractIndexPage findPage(AbstractIndexPage currentPage, short pagePos) {
			return currentPage.readPage(pagePos, pageClones);
		}

		/**
		 * Check whether the keys on the page overlap with the min/max/current values of this 
		 * iterator.
		 */
		abstract boolean pageIsRelevant(AbstractIndexPage page);
	
		public void close() {
			ind.deregisterIterator(this);
		}
		
		protected boolean isUnique() {
			return ind.isUnique();
		}
	}
	
	

	protected transient final int maxLeafN;
	/** Max number of keys in inner page (there can be max+1 page-refs) */
	protected transient final int maxInnerN;
	//TODO if we ensure that maxXXX is a multiple of 2, then we could scrap the minXXX values
	/** minLeafN = maxLeafN >> 1 */
	protected transient final int minLeafN;
	/** minInnerN = maxInnerN >> 1 */
	protected transient final int minInnerN;
	protected final PageAccessFile paf;
	protected int statNLeaves = 0;
	protected int statNInner = 0;
	protected int statNWrittenPages = 0;
	
	protected final int keySize;
	protected final int valSize;
	
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
	public AbstractPagedIndex(PageAccessFile raf, boolean isNew, int keyLen, int valLen,
	        boolean isUnique) {
		super(raf, isNew, isUnique);
		
		paf = raf;
		int pageSize = paf.getPageSize();
		
		keySize = keyLen;
		valSize = valLen;
		
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
		maxLeafN = (pageSize - pageHeader) / (keyLen + valLen);
		if (maxLeafN * (keyLen + valLen) + pageHeader > pageSize) {
			throw new JDOFatalDataStoreException("Illegal Index size: " + maxLeafN);
		}
		minLeafN = maxLeafN >> 1;
		
		int innerEntrySize = keyLen + refLen;
		if (!isUnique) {
			innerEntrySize += valLen;
		}
		//-2 for short nKeys
		maxInnerN = (pageSize - pageHeader - refLen - 2) / innerEntrySize;
		if (maxInnerN * innerEntrySize + pageHeader + refLen > pageSize) {
			throw new JDOFatalDataStoreException("Illegal Index size: " + maxInnerN);
		}
		minInnerN = maxInnerN >> 1;
	}

	abstract AbstractIndexPage createPage(AbstractIndexPage parent, boolean isLeaf);

	public final int write() {
		if (!getRoot().isDirty()) {
			return getRoot().pageId();
		}
		
		return getRoot().write();
	}

	/**
	 * Method to preallocate pages for a write command.
	 * @param map
	 */
	final void preallocatePagesForWriteMap(Map<AbstractIndexPage, Integer> map, 
			FreeSpaceManager fsm) {
		getRoot().createWriteMap(map, fsm);
	}
	
	/**
	 * Special write method that uses only pre-allocated pages.
	 * @param map
	 * @return the root page Id.
	 */
	final int writeToPreallocated(Map<AbstractIndexPage, Integer> map) {
		return getRoot().writeToPreallocated(map);
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
		paf.seekPageForRead(pageId, false);
		int nL = paf.readShort();
		AbstractIndexPage newPage;
		if (nL == 0) {
			newPage = createPage(parentPage, true);
			newPage.readData();
		} else {
			newPage = createPage(parentPage, false);
			paf.noCheckRead(newPage.leafPages);
			newPage.readKeys();
		}
		newPage.setPageId( pageId );  //the page ID is for exampled used to return the page to the FSM
		newPage.setDirty( false );
		return newPage;
	}

	protected abstract void updateRoot(AbstractIndexPage newRoot);

	public int statsGetInnerN() {
		return statNInner;
	}

	public int statsGetLeavesN() {
		return statNLeaves;
	}
	
	public int statsGetWrittenPagesN() {
		return statNWrittenPages;
	}
	
	private AbstractPageIterator<?> registerIterator(AbstractPageIterator<?> iter) {
		iterators.put(iter, new Object());
		return iter;
	}
	
	/**
	 * This is automatically called when the iterator finishes. But it can be called manually
	 * if iteration is aborted before the end is reached.
	 * 
	 * @param iter
	 */
	public void deregisterIterator(AbstractPageIterator<?> iter) {
		iterators.remove(iter);
	}

	final void notifyPageUpdate(AbstractIndexPage page) {
        AbstractIndexPage clone = null;
        for (AbstractPageIterator<?> indexIter: iterators.keySet()) {
            clone = indexIter.pageUpdateNotify(page, clone, modcount);
        }
	}
}
