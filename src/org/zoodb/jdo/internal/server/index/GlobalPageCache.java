package org.zoodb.jdo.internal.server.index;

import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.AbstractIndexPage;

/**
 * When index pages are loaded they are put into this cache. Why?
 * 
 * This list is useful when a inner page gets changed where certain child-pages have not been loaded 
 * yet. When the main index or one of the COW-iterators loads the sub-page, than it is put here,
 * so others do not reload it unnecessarily, if they need it.
 * 
 * What happens to pages that get modified? We could:
 * - remove the page from this list, meaning that it may be reloaded from disk-cache if someone else
 *   needs it.
 * - or we could preemptively clone the page before modifying it, meaning that we always have this 
 *   overhead of cloning it.
 * Optimization: Only clone if iterators are registered, and if the page to clone lies between
 * current position and maximum key of this iterator. -> Good idea! TODO
 * If a new iterator is created, it won't need the cloned page, because it will look at the new 
 * version.
 * 
 * Problem: how to handle different versions? Which version does a certain iterator need?
 * -> hashcode: 32bit pageID + 32bit modifiercount???-> no, we need modifiercount to be equal OR
 *    NEXT HIGHER than that of the iterator! -> Maintain list of pages.
 *    
 * GC: Here we use weak references. But for older iterators (version newer than disk but older than
 *     present) we need hard references. Actually, obviously the latest version needs a hard 
 *     reference as well. -> Simply maintain hard-ref a list of all dirty pages? -> if all iterators
 *     with their modcounts are registered, than the list can be cleaned up from pages with older 
 *     modcount. -> to recognize iterators w/o close, keep only a soft reference of all iterators
 *     and their modcount.
 *     
 *     
 * Can this system be used cross-session? I think so.
 * 
 * 
 * @author Tilmann Zäschke
 *
 */
public final class GlobalPageCache {
	//TODO the following needs to be maintained per index!!

	//loaded pages
	//Why use a weak hashMap? The advantage is that if the pages are only hard-referenced by the
	//iterators, then we do not need to worry about cleaning them up here, which may be hard.
	//But, certain pages may get lost is they are not referenced by any iterator and not yet used 
	//by the new one!!
	// -> We need a HARD referenced list of all modifi ?!?!?!?!
	xxxxx -> rethink
	
	//What about the following:
	//Each iterator has a list of pages it may need (cleaned-up while iterating to only reference
	//'ahead' pages. Any modified page gets added to the lists of all existing iterators, unless 
	//they already reference that page. The list (and all referencing pages) are gc'd when the
	//iterator is gc'd (hard reference form the iterator).
	//Impl-detail: Index passes iterator over Iterators and this-pointer to first iterator.
	//if the iterator wants the pages, it clones it and adds it to the local list.
	//If it created a clone, then the clone is passed on to the next iterator.
	//The list of iterators should be a weak list.
	//-> How to use cross-session.
	
	// <modcount, list of pages>
	private final ConcurrentHashMap<Integer, WeakHashMap<AbstractIndexPage, Object>> allIndexPages =
		new ConcurrentHashMap<Integer, WeakHashMap<AbstractIndexPage,Object>>();
	
	//this variable can be compared to the lowest integer in the _iterators map to see when 
	//a clean-up is indicated.
	private int lowestIteratorModCount = Integer.MAX_VALUE;
	private final WeakHashMap<Iterator<?>, Integer> _iterators = 
		new WeakHashMap<Iterator<?>, Integer>();
	
}
