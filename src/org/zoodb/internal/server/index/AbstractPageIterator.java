/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import java.util.IdentityHashMap;
import java.util.Map;

public abstract class AbstractPageIterator<E> implements LongLongIndex.LongLongIterator<E> {
	protected final AbstractPagedIndex ind;
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
			
			//now we need to identify the parent and make sure that the parent contains a link
			//to this page. The problem is, that this page may have been loaded only after
			//the parent was cloned, so the parent may not have a reference to this page and
			//may attempt to load it again.
			//But this could fail if this page has been committed in the meantime and changed 
			//its position. Therefore we need to make sure that the parent has a link to this 
			//page.
			// .... why is clone.parent = null???? Why do we set it to nul??
			if (page.getParent() != null && pageClones.get(page.getParent()) != null) {
			    AbstractIndexPage parentClone = pageClones.get(page.getParent());
			    for (int i = 0; i <= parentClone.getNKeys(); i++) {
			        //This is the first time (since this iterator is created) that 'page' is
			        //cloned, therefore the pageId should be correct.
			        if (parentClone.subPageIds[i] == page.pageId()) {
			            //Why do index-tests fail if we don't check for null???
			            if (parentClone.subPages[i] == null) {
			                parentClone.subPages[i] = clone;
			            }
			            break;
			        }
			    }
			}
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

	@Override
	public void close() {
		ind.deregisterIterator(this);
	}
	
	protected boolean isUnique() {
		return ind.isUnique();
	}

	/**
	 * Refresh the iterator (clear COW copies).
	 */
    @Override
	public final void refresh() {
        pageClones.clear();
        reset();
    }
    
    abstract void reset();
}