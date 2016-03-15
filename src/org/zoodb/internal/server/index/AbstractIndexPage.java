/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.server.index;

import java.util.Arrays;
import java.util.Map;

import org.zoodb.internal.util.DBLogger;


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
 * @author Tilmann Zaeschke
 */
abstract class AbstractIndexPage {

	protected final AbstractPagedIndex ind;
	private transient boolean isDirty;
	final transient boolean isLeaf;
	final AbstractIndexPage[] subPages;
	final int[] subPageIds;
	private int pageId = -1;
	
	
	AbstractIndexPage(AbstractPagedIndex ind, AbstractIndexPage parent, boolean isLeaf) {
		this.ind = ind;
		if (!isLeaf) {	
			subPages = new AbstractIndexPage[ind.maxInnerN + 1];
			subPageIds = new int[ind.maxInnerN + 1];
			ind.statNInner++;
		} else {
			subPages = null;
			subPageIds = null;
			ind.statNLeaves++;
		}
		this.isLeaf = isLeaf;

		//new pages are always dirty
		setDirty( true );
	}

	protected abstract AbstractIndexPage newInstance();

	/**
	 * Copy constructor.
	 * @param p
	 */
	AbstractIndexPage(AbstractIndexPage p) {
		ind = p.ind;
		setDirty( p.isDirty() );
		isLeaf = p.isLeaf;
		if (!isLeaf) {
			subPageIds = p.subPageIds.clone();
			subPages = p.subPages.clone();
		} else {
			subPageIds = null;
			subPages = null;
		}
		pageId = p.pageId;
	}

	private final void markPageDirty() {
		//if the page is already dirty, then the parent is as well.
		//no further action is necessary. Parent and index wrapper are already cloned and dirty.
		if (!isDirty()) {
            setDirty( true );
            if (getParent() != null) {
                //Don't clone parent. Clone only if parent actually changes.
                getParent().markPageDirty();
            } else {
                //this is root, mark the wrapper dirty.
                ind.markDirty();
            }
		}

		//The cloning is always initiated from the methods (add/remove element) that modify a 
		//page. For inner pages, modifying one of their leaves does not require a clone, because
		//the page itself does not change, and the leaves are always looked up via the 
		//iterator's clone list.
		
		//For the leaves, creating a new parent clone does not matter, because the 'parent'
		//field is not used by iterators.
	}

	protected final void markPageDirtyAndClone() {
		ind.notifyPageUpdate();
        
        //Now mark this page and its parents as dirty.
        //The parents must be dirty to force a rewrite. They must always be rewritten, because
        //the reference to the leaf-pages changes when a leaf gets rewritten.
        //Using and ID registry for leaf pages to avoid this problem does not help, because
        //the registry would then need updating as well (reducing the benefit) and above all
        //the registry itself cannot depend on another registry. In the end, this index here
        //would be the registry.
        markPageDirty();
	}
	
	
	/** number of keys. There are nEntries+1 subPages in any leaf page. */
	abstract short getNKeys();

	protected final AbstractIndexPage readPage(int pos) {
		return readOrCreatePage(pos, false);
	}
	
	
	protected final AbstractIndexPage readOrCreatePage(int pos, boolean allowCreate) {
		AbstractIndexPage page = subPages[pos];
		if (page != null) {
			//page is in memory
			return page;
		}
		
		//now try to load it
		int pageId = subPageIds[pos];
		if (pageId == 0) {
			if (!allowCreate) {
				return null;
			}
			//create new page
			page = ind.createPage(this, true);
			// we have to perform this makeDirty here, because calling it from the new Page
			// will not work because it is already dirty.
			markPageDirtyAndClone();
			incrementNEntries();
		} else {
			//load page
			page = ind.readPage(pageId, this);
		}
		subPages[pos] = page;
		return page;
	}
	
	protected abstract void incrementNEntries();
	
	final AbstractIndexPage readCachedPage(short pos) {
	    AbstractIndexPage page = subPages[pos];
		if (page != null) {
			//page is in memory
			return page;
		}
		
		return readPage(pos);
	}
	
	
	final int write() {
		if (!isDirty()) {
			return pageId;
		}

		if (isLeaf) {
			pageId = ind.out.allocateAndSeek(ind.getDataType(), pageId);
			ind.out.writeShort((short) 0);
			writeData();
		} else {
			//first write the sub pages, because they will update the page index.
			for (int i = 0; i < getNKeys()+1; i++) {
				AbstractIndexPage p = subPages[i];
				if (p == null) {
					//This can happen if pages are not loaded yet
					continue;
				}
				subPageIds[i] = p.write();
			}
			//TODO optimize: find a way to first write the inner nodes, then the leaves. Could
			//     be faster when reading the index. -> SDD has no such problem !!?!??
			
			//now write the page index
			pageId = ind.out.allocateAndSeek(ind.getDataType(), pageId);
			ind.out.writeShort((short) subPages.length);
			ind.out.noCheckWrite(subPageIds);
			writeKeys();
		}
		ind.out.flush();
		setDirty( false );
		ind.statNWrittenPages++;
		return pageId;
	}

	final int createWriteMap(Map<AbstractIndexPage, Integer> map, FreeSpaceManager fsm) {
		if (!isDirty()) {
			return pageId;
		}

		if (!map.containsKey(this)) {
			//avoid seek here! Only allocate!
			pageId = fsm.getNextPageWithoutDeletingIt(pageId);
			map.put(this, pageId);
		}
		if (!isLeaf) {
			//first write the sub pages, because they will update the page index.
			for (int i = 0; i < getNKeys()+1; i++) {
				AbstractIndexPage p = subPages[i];
				if (p == null) {
					continue;
				}
				subPageIds[i] = p.createWriteMap(map, fsm);
			}
		}
		return pageId;
	}
	
	/**
	 * This method takes as argument a map generated by createWriteMap(). These methods are 
	 * especially created for the free space manager. But it also enables other indices to
	 * first write the inner nodes followed by leaf nodes.
	 * This write method does not allocate any pages, but takes pre-allocated pages from the
	 * supplied map. This is necessary for the free space manager, because allocating new
	 * pages during the write() process would change the free space manager again, because
	 * allocation is often associated with releasing a previously used page.
	 * @param map
	 * @return new page ID
	 */
	final int writeToPreallocated(Map<AbstractIndexPage, Integer> map) {
		if (!isDirty()) {
			return pageId;
		}

		Integer pageIdpre = map.get(this); 
		if (pageIdpre == null) {
			throw DBLogger.newFatalInternal("Page not preallocated: " + pageId + " / " + this);
		}
		
		if (isLeaf) {
			//Page was already reported to FSM during map build-up
			ind.out.seekPageForWrite(ind.getDataType(), pageId);
			ind.out.writeShort((short) 0);
			writeData();
		} else {
			//now write the sub pages
			for (int i = 0; i < getNKeys()+1; i++) {
				AbstractIndexPage p = subPages[i];
				if (p == null) {
					//This can happen if pages are not loaded yet
					continue;
				}
				subPageIds[i] = p.writeToPreallocated(map);
			}

			//now write the page index
			ind.out.seekPageForWrite(ind.getDataType(), pageId);
			ind.out.writeShort((short) subPages.length);
			ind.out.noCheckWrite(subPageIds);
			writeKeys();
		}
		ind.out.flush();
		setDirty( false );
		ind.statNWrittenPages++;
		return pageId;
	}

	abstract void writeKeys();

	abstract void writeData();

	abstract void readData();

	public abstract void print(String indent);
	
	abstract AbstractIndexPage getParent();

	abstract void setParent(AbstractIndexPage parent);

	/**
	 * Returns only INNER pages.
	 * TODO for now this ignores leafPages on a previous inner node. It returns only leaf pages
	 * from the current node.
	 * @param indexPage
	 * @return The previous leaf page or null, if the given page is the first page.
	 */
	final protected AbstractIndexPage getPrevInnerPage(AbstractIndexPage indexPage) {
		int pos = getPagePosition(indexPage);
		if (pos > 0) {
			AbstractIndexPage page = getPageByPos(pos-1);
			if (page.isLeaf) {
				return null;
			}
			return page;
		}
		if (getParent() == null) {
			return null;
		}
		//TODO we really should return the last leaf page of the previous inner page.
		//But if they get merged, then we have to shift minimal values, which is
		//complicated. For now we ignore this case, hoping that it doesn't matter much.
		return null;
	}

	/**
	 * Returns only LEAF pages.
	 * TODO for now this ignores leafPages on a previous inner node. It returns only leaf pages
	 * from the current node.
	 * @param indexPage
	 * @return The previous leaf page or null, if the given page is the first page.
	 */
	final protected AbstractIndexPage getPrevLeafPage(AbstractIndexPage indexPage) {
		int pos = getPagePosition(indexPage);
		if (pos > 0) {
			AbstractIndexPage page = getPageByPos(pos-1);
			return page.getLastLeafPage();
		}
		if (getParent() == null) {
			return null;
		}
		//TODO we really should return the last leaf page of the previous inner page.
		//But if they get merged, then we have to shift minimal values, which is
		//complicated. For now we ignore this case, hoping that it doesn't matter much.
		return null;
	}

	/**
	 * Returns only LEAF pages.
	 * TODO for now this ignores leafPages on other inner nodes. It returns only leaf pages
	 * from the current node.
	 * @param indexPage
	 * @return The previous next page or null, if the given page is the first page.
	 */
	final protected AbstractIndexPage getNextLeafPage(AbstractIndexPage indexPage) {
		int pos = getPagePosition(indexPage);
		if (pos < getNKeys()) {
			AbstractIndexPage page = getPageByPos(pos+1);
			return page.getFirstLeafPage();
		}
		if (getParent() == null) {
			return null;
		}
		//TODO we really should return the last leaf page of the previous inner page.
		//But if they get merged, then we have to shift minimal values, which is
		//complicated. For now we ignore this case, hoping that it doesn't matter much.
		return null;
	}

	/**
	 * 
	 * @return The first leaf page of this branch.
	 */
	private AbstractIndexPage getFirstLeafPage() {
		if (isLeaf) {
			return this;
		}
		return readPage(0).getFirstLeafPage();
	}

	/**
	 * 
	 * @return The last leaf page of this branch.
	 */
	private AbstractIndexPage getLastLeafPage() {
		if (isLeaf) {
			return this;
		}
		return readPage(getNKeys()).getLastLeafPage();
	}

	/**
	 * Returns (and loads, if necessary) the page at the specified position.
	 */
	protected AbstractIndexPage getPageByPos(int pos) {
		AbstractIndexPage page = subPages[pos];
		if (page != null) {
			return page;
		}
		page = ind.readPage(subPageIds[pos], this);
		subPages[pos] = page;
		return page;
	}

	/**
	 * This method will fail if called on the first page in the tree. However this should not
	 * happen, because when called, we already have a reference to a previous page.
	 * @param oidIndexPage
	 * @return The position of the given page in the subPage-array with 0 <= pos <= nEntries.
	 */
	int getPagePosition(AbstractIndexPage indexPage) {
		//We know that the element exists, so we iterate to list.length instead of nEntires 
		//(which is not available in this context at the moment.
		for (int i = 0; i < subPages.length; i++) {
			if (subPages[i] == indexPage) {
				return i;
			}
		}
		throw DBLogger.newFatalInternal("Leaf page not found in parent page: " + 
				indexPage.pageId + "   " + Arrays.toString(subPageIds));
	}

	public abstract void printLocal();
	
	protected void assignThisAsRootToLeaves() {
		for (int i = 0; i <= getNKeys(); i++) {
			//leaves may be null if they are not loaded!
			if (subPages[i] != null) {
				subPages[i].setParent(this);
			}
		}
	}

	abstract void readKeys();
	
	protected int pageId() {
		return pageId;
	}

	/**
	 * @return Minimal key on this branch.
	 */
	abstract long getMinKey();

	/**
	 * @return Value of minimal key on this branch.
	 */
	abstract long getMinKeyValue();
	
	final boolean isDirty() {
		return isDirty;
	}

	final void setDirty(boolean isDirty) {
		this.isDirty = isDirty;
	}
	
	final void setPageId(int pageId) {
		this.pageId = pageId;
	}

	final void clear() {
		if (!isLeaf) {
			for (int i = 0; i < getNKeys()+1; i++) {
				AbstractIndexPage p = readPage(i);
				p.clear();
				//0-IDs are automatically ignored.
				ind.file.reportFreePage(p.pageId);
			}
		}
		if (subPageIds != null) {
			for (int i = 0; i < subPageIds.length; i++) {
				subPageIds[i] = 0;
			}
		}
		if (subPages != null) {
			for (int i = 0; i < subPages.length; i++) {
				subPages[i] = null;
			}
		}
		setNEntries(-1);
		setDirty(true);
	}

	abstract void setNEntries(int n);
}
