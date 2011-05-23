package org.zoodb.jdo.internal.server.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

/**
 * @author Tilmann Zäschke
 */
public abstract class AbstractPagedIndex extends AbstractIndex {

	public interface LongLongIndex {
		void insertLong(long key, long value);

		AbstractPageIterator<LLEntry> iterator(long minValue, long maxValue);

		boolean removeLong(long key, long value);
	}
	
	public abstract static class AbstractPageIterator<E> implements CloseableIterator<E> {
		private final AbstractPagedIndex ind;
		//TODO use different map to accommodate large numbers of pages?
		//We only have a map with original<->clone associations.
		//There can only be a need to clone a page if it has been modified. If it has been modified,
		//it must have been loaded and should be in the list of loaded leaf-pages.
		private final Map<AbstractIndexPage, AbstractIndexPage> pageClones = 
			new HashMap<AbstractIndexPage, AbstractIndexPage>();
		
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
					clone.parent = null; //pageClones.get(page.parent);
				}
				pageClones.put(page, clone);
				clone.original = page;
				//maybe we are using it right now?
				replaceCurrentAndStackIfEqual(page, clone);
			}
			//this can still be null
			return clone;
		}

		abstract void replaceCurrentAndStackIfEqual(AbstractIndexPage equal, AbstractIndexPage replace);
		
		protected final void releasePage(AbstractIndexPage oldPage) {
			//just try it, even if it is not in the list.
			if (pageClones.remove(oldPage.original) == null && oldPage.original!=null) {
			    System.out.println("Cloned page not found!");
			}
		}
		
		protected final AbstractIndexPage findPage(AbstractIndexPage currentPage, short pagePos) {
			return currentPage.readOrCreatePage(pagePos, pageClones);
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
	
	
	/**
	 * In the inner pages, the keys are the minimum values of the following page.
 	 * 
 	 * TODO
 	 * To avoid special cases, the tree should be changed as follows:
     * - the empty tree should consist of one empty leaf and one node without keys. Then we could
     *   avoid the following special cases:
     *   - the parent of a leaf is never null, there is always a parent node.
     *   - a inner node is never empty. That means (nLeaf)===(nEntries+1).
     *   This means special treating when creating or deleting from the root node, but the
     *   iterators and internal navigation should be easier. Disadvantage: empty index has two 
     *   pages. -> Should not be a problem, because who creates empty indices? Or classes w/o
     *   instances?
	 * 
	 * @author Tilmann Zäschke
	 */
	protected abstract static class AbstractIndexPage {

		protected final AbstractPagedIndex ind;
		transient boolean isDirty;
		final transient boolean isLeaf;
		AbstractIndexPage parent;
		final AbstractIndexPage[] leaves;
		final int[] leafPages;
		private int pageId = -1;
		//this is a pointer to the original page, in case this is a clone.
		private AbstractIndexPage original;
		
		
		AbstractIndexPage(AbstractPagedIndex ind, AbstractIndexPage parent, boolean isLeaf) {
			this.ind = ind;
			this.parent = parent;
			if (!isLeaf) {	
				leaves = new AbstractIndexPage[ind.maxInnerN + 1];
				leafPages = new int[ind.maxInnerN + 1];
				ind.statNInner++;
			} else {
				leaves = null;
				leafPages = null;
				ind.statNLeaves++;
			}
			this.isLeaf = isLeaf;

			//new pages are always dirty
			isDirty = true;
		}

		protected abstract AbstractIndexPage newInstance();

		/**
		 * Copy constructor.
		 * @param p
		 */
		AbstractIndexPage(AbstractIndexPage p) {
			ind = p.ind;
			isDirty = p.isDirty;
			isLeaf = p.isLeaf;
			if (!isLeaf) {
				leafPages = p.leafPages.clone();
				leaves = p.leaves.clone();
			} else {
				leafPages = null;
				leaves = null;
			}
			pageId = p.pageId;
			parent = p.parent;
		}

		private final void markPageDirty() {
            //First we need to make parent pages dirty, because the clone() in the iterators needs
            //cloned parent pages to be present.
            //Also, we need to do this, even if the parent is already dirty, because there may be
            //new iterators around that need a new clone.
            isDirty = true;
            if (parent != null) {
                parent.markPageDirtyAndClone();
            } else {
                //this is root, mark the wrapper dirty.
                ind.markDirty();
            }
		}

		protected final void markPageDirtyAndClone() {
            //always create clone, even if page is already dirty
		    //however we clone only this page, not the parent. Cloning is only required if a page
		    //changes in memory, that is, if a leaf or element is added or removed.
            AbstractIndexPage clone = null;
            for (AbstractPageIterator<?> indexIter: ind.iterators.keySet()) {
                clone = indexIter.pageUpdateNotify(this, clone, ind.modcount);
            }
            
            //Discussion on reducing page cloning
            //We could only clone pages that have changed, avoiding cloning the parent pages.
            //This would require some refactoring (put the code before the clone-loop) into a 
            //separate method.
            //In the pageUpdateNotify, we have to take care that we set parent only if the
            //parent is already cloned. If we clone a parent, we have to take care that we update
            //the parent-pointer of all leaf pages, but only if they are clones(!).
            //-> this seems complicated, and there is little to gain. (only iterators that are 
            //   alive while very few matching elements are added/removed
            //--> May become an issue in highly concurrent environments or when iterators are not 
            //closed(!)
            
            //RESULT: For now, we do not clone it, because the clones do not need parent pages!
            //x.parent is set to null in pageUpdateNotify().
            
            //Now mark this page and its parents as dirty.
            //The parents must be dirty to force a rewrite. They must always be rewritten, because
            //the reference to the leaf-pages changes when a leaf gets rewritten.
            //Using and ID registry for leaf pages to avoid this problem does not help, because
            //the registry would then need updating as well (reducing the benefit) and above all
            //the registry itself can not depend on another registry. IN the end, this index here
            //would be the registry.
            markPageDirty();
		}
		
		
//		protected final void markPageDirtyAndClone() {
//            //always create clone, even if page is already dirty
//		    //however we clone only this page, not the parent. Cloning is only required if a page
//		    //changes in memory, that is, if a leaf or element is added or removed.
//            AbstractIndexPage clone = null;
//            for (AbstractPageIterator<?> indexIter: ind.iterators.keySet()) {
//                clone = indexIter.pageUpdateNotify(this, clone, ind.modcount);
//            }
//            
//            //If there was no need to clone this page then there is no need to clone any parent 
//            //page.
//            //TODO we could be more precise here. After commit, page might be non-dirty, but
//            //clones are still required. Move down and merge with 'if' in markDirty(). And 
//            //markDirty may be required even if clone is not required.
////            if (isDirty && clone == null) {
////            	return;
////            }
//
//            //Discussion on reducing page cloning
//            //We could only clone pages that have changed, avoiding cloning the parent pages.
//            //This would require some refactoring (put the code before the clone-loop) into a 
//            //separate method.
//            //In the pageUpdateNotify, we have to take care that we set parent only if the
//            //parent is already cloned. If we clone a parent, we have to take care that we update
//            //the parent-pointer of all leaf pages, but only if they are clones(!).
//            //-> this seems complicated, and there is little to gain. (only iterators that are 
//            //   alive while very few matching elements are added/removed
//            //--> May become an issue in highly concurrent environments or when iterators are not 
//            //closed(!)
//            
//            //RESULT: For now, we do not clone it, because the clones do not need parent pages!
//            //x.parent is set to null in pageUpdateNotify().
//            
//            //Now mark this page and its parents as dirty.
//            //The parents must be dirty to force a rewrite. They must always be rewritten, because
//            //the reference to the leaf-pages changes when a leaf gets rewritten.
//            //Using and ID registry for leaf pages to avoid this problem does not help, because
//            //the registry would then need updating as well (reducing the benefit) and above all
//            //the registry itself can not depend on another registry. IN the end, this index here
//            //would be the registry.
//            
//            //First we need to make parent pages dirty, because the clone() in the iterators needs
//            //cloned parent pages to be present.
//            //Also, we need to do this, even if the parent is already dirty, because there may be
//            //new iterators around that need a new clone.
////            if (!isDirty) {
//                isDirty = true;
//                if (parent != null) {
//                    parent.markPageDirtyAndClone();
//                } else {
//                    //this is root, mark the wrapper dirty.
//                    ind.markDirty();
//                }
////                if (parent == null) {
//////                	parent.markPageDirtyAndClone();
//////                } else {
////                	//this is root, mark the wrapper dirty.
////                	ind.markDirty();
////                }
////            }
////            if (parent != null) {
////            	parent.markPageDirtyAndClone();
////            }
//		}
		
		
		protected final AbstractIndexPage readOrCreatePage(short pos, boolean allowCreate) {
			AbstractIndexPage page = leaves[pos];
			if (page != null) {
				//page is in memory
				//TODO take care it's not cached in ByteBuffer cache (double caching)
				//TODO take care it's not gc'd before a pageId is allocated
				return page;
			}
			
			//now try to load it
			int pageId = leafPages[pos];
			if (pageId == 0) {
				if (!allowCreate) {
					return null;
				}
				//create new page
				page = ind.createPage(this, true);
			} else {
				//load page
				page = ind.readPage(pageId, this);
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
				page = ind.createPage(this, true);
			} else {
				//load page
				page = ind.readPage(pageId, this);
			}
			leaves[pos] = page;
			return page;
		}
		
		
		final int write() {
			if (!isDirty) {
				return pageId;
			}

			if (isLeaf) {
				pageId = ind.paf.allocateAndSeek(false);
				ind.paf.writeShort((short) 0);
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
				pageId = ind.paf.allocateAndSeek(false);
				ind.paf.writeShort((short) leaves.length);
				ind.paf.noCheckWrite(leafPages);
				writeKeys();
			}
			isDirty = false;
			return pageId;
		}


		abstract void writeKeys();

		abstract void writeData();

		abstract void readData();

		abstract void addSubPage(AbstractIndexPage newPage, long minKey, long minValue);

		public abstract void print(String indent);

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
			if (parent == null) {
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
			page = ind.readPage(leafPages[pos], this);
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
			throw new JDOFatalDataStoreException("Leaf page not found in parent page: " + indexPage.pageId + 
					"   " + Arrays.toString(leafPages));
		}

		public abstract void printLocal();
		
		protected void assignThisAsRootToLeaves() {
			for (AbstractIndexPage leaf: leaves) {
				//TODO improve to avoid checking ALL entries?
				//leaves may be null if they are not loaded!
				if (leaf != null) {
					leaf.parent = this;
				}
			}
		}

		abstract void readKeys();
		
		protected int pageId() {
			return pageId;
		}
	}

	protected transient final int maxLeafN;
	//Max number of keys in inner page (there can be max+1 page-refs)
	protected transient final int maxInnerN;
	//TODO if we ensure that maxXXX is a multiple of 2, then we could scrap the minXXX values
	protected transient final int minLeafN;
	protected transient final int minInnerN;
	protected final PageAccessFile paf;
	protected int statNLeaves = 0;
	protected int statNInner = 0;
	
	
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
		paf.seekPage(pageId, false);
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
		newPage.pageId = pageId;  //TODO check what this is good for...
		return newPage;
	}

	protected abstract void updateRoot(AbstractIndexPage newRoot);

	public int statsGetInnerN() {
		return statNInner;
	}

	public int statsGetLeavesN() {
		return statNLeaves;
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
}
