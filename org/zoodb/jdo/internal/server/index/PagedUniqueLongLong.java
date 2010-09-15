package org.zoodb.jdo.internal.server.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

public class PagedUniqueLongLong extends AbstractPagedIndex {
	
	static class LLEntry {
		final long key;
		final long value;
		private LLEntry(long k, long v) {
			key = k;
			value = v;
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

	/**
	 * Some thoughts on Iterators:
	 * 
	 * JDO has a usecase like this:
	 * Iterator iter = extent.iterator();
	 * while (iter.hasNext()) {
	 * 	   pm.deletePersistent(iter.next());
	 * }
	 * 
	 * That means:
	 * The iterator needs to support deletion without introducing duplicates and without skipping 
	 * objects. It needs to be a perfect iterator.
	 * 
	 * According to the spec 2.2., the extent should contain whatever existed a the time of the 
	 * execution of the query or creation of the iterator (JDO 2.2).
	 * 
	 * So:
	 * - Different sessions should use COW to create locally valid 'copies' of the traversed index.
	 * - Within the same session, iterators should support deletion as described above.
	 * 
	 * To support the deletion locally, there are several option:
	 * - one could use COW as well, which would mean that bidirectional iterators would not work,
	 *   because the iterator iterates over copies of the original list. 
	 *   Basically the above example would work, but deletions ahead of the iterator would not
	 *   be recognized (desirable?). TODO Check spec.
	 * - Alternative: Update iterator with callbacks from index modification.
	 *   This would mean ahead-of-iterator modifications would be recognized (desirable?)
	 *   
	 *    
	 * 
	 * @author Tilmann Zäschke
	 *
	 */
	private class ULLIterator extends AbstractPageIterator<LLEntry> {

		private ULLIndexPage currentPage;
		private short currentPos = 0;
		private final long minKey;
		private final long maxKey;
		private final Stack<IteratorPos> stack = new Stack<IteratorPos>();
		
		public ULLIterator(ULLIndexPage root, long minKey, long maxKey) {
			super();
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
			currentPage = (ULLIndexPage) ip.page;
			currentPos = ip.pos;
			currentPos++;
			return navigateToNextLeaf();
		}

		private boolean navigateToNextLeaf() {
			while (currentPos > currentPage.nEntries) {
				releasePage(currentPage);
				if (stack.isEmpty()) {
					return false;
				}
				IteratorPos ip = stack.pop();
				currentPage = (ULLIndexPage) ip.page;
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
				currentPage = (ULLIndexPage) findPage(currentPage, currentPos);
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
		public LLEntry next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			//hasNext should leave us on a leaf page
			currentPos++;
			return new LLEntry(currentPage.keys[currentPos], currentPage.values[currentPos]);
		}

		@Override
		public void remove() {
			//As defined in the JDO 2.2. spec:
			throw new UnsupportedOperationException();
		}
		
		/**
		 * This method may not be called if the iterator is used in 'for ( : ext) {}' constructs! 
		 */
		public void close() {
			// after close() everything should throw NoSuchElementException (see 2.2. spec)
			currentPage = null;
		}

		@Override
		boolean pageIsRelevant(AbstractIndexPage aiPage) {
			ULLIndexPage page = (ULLIndexPage) aiPage;
			if (page == currentPage) {
				return true;
			}
			if (page.root == null) {
				//if anything has been cloned, then the root page has been cloned as well.
				return true;
			}
			ULLIndexPage parent = (ULLIndexPage) page.root;
			int pPos = parent.getPagePosition(page);
			//TODO specific to forward iterator
			if (page.keys[currentPos] > parent.keys[]) {
				return false;
			}
			//check min max
			if (maxKey < parent.keys[]) {
				return false;
			}
			return true;
		}

		@Override
		void replaceCurrentIfEqual(AbstractIndexPage equal,
				AbstractIndexPage replace) {
			if (currentPage == equal) {
				currentPage = (ULLIndexPage) replace;
			}
		}
	}
	
	class ULLDescendingIterator extends AbstractPageIterator<LLEntry> {

		private ULLIndexPage currentPage;
		private short currentPos = 0;
		private final long minKey;
		private final long maxKey;
		private final Stack<IteratorPos> stack = new Stack<IteratorPos>();
		
		public ULLDescendingIterator(ULLIndexPage root, long maxKey, long minKey) {
			super();
			//find page
			currentPage = root;
			xxxx
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
			currentPage = (ULLIndexPage) ip.page;
			currentPos = ip.pos;
			currentPos++;
			return navigateToNextLeaf();
		}

		private boolean navigateToNextLeaf() {
			while (currentPos > currentPage.nEntries) {
				releasePage(currentPage);
				if (stack.isEmpty()) {
					return false;
				}
				IteratorPos ip = stack.pop();
				currentPage = (ULLIndexPage) ip.page;
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
				currentPage = (ULLIndexPage) findPage(currentPage, currentPos);
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
		public LLEntry next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			//hasNext should leave us on a leaf page
			currentPos++;
			return new LLEntry(currentPage.keys[currentPos], currentPage.values[currentPos]);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		boolean pageIsRelevant(AbstractIndexPage aiPage) {
			ULLIndexPage page = (ULLIndexPage) aiPage;
			if (page == currentPage) {
				return true;
			}
			if (page.root == null) {
				//if anything has been cloned, then the root page has been cloned as well.
				return true;
			}
			ULLIndexPage parent = (ULLIndexPage) page.root;
			int pPos = parent.getPagePosition(page);
			//specific to backward iterator
			if (page.keys[currentPos] < parent.keys[]) {
				return false;
			}
			//check min max
			if (minKey > parent.keys[]) {
				return false;
			}
			return true;
		}

		@Override
		void replaceCurrentIfEqual(AbstractIndexPage equal,
				AbstractIndexPage replace) {
			if (currentPage == equal) {
				currentPage = (ULLIndexPage) replace;
			}
		}
	}
	
	class ULLIndexPage extends AbstractIndexPage {
		private final long[] keys;
		//TODO store only pages or also offs? -> test de-ser whole page vs de-ser single obj.
		//     -> especially, objects may not be valid anymore (deleted)! 
		private final long[] values;
		//transient final PageAccessFile paf;
		/** number of keys. There are nEntries+1 subPages in any leaf page. */
		short nEntries;
		
		public ULLIndexPage(AbstractIndexPage root, boolean isLeaf) {
			super(root, isLeaf);
			if (isLeaf) {
				keys = new long[maxLeafN];
				values = new long[maxLeafN];
			} else {
				keys = new long[maxInnerN];
				values = null;
			}
		}

		@Override
		public ULLIndexPage clone() {
			ULLIndexPage page = (ULLIndexPage) super.clone();
			System.arraycopy(keys, 0, page.keys, 0, page.keys.length);
			nEntries = page.nEntries;
			System.arraycopy(values, 0, page.values, 0, values.length);
			return page;
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

		public ULLIndexPage locatePageForKey(long key) {
			if (isLeaf) {
				return this;
			}
			//The stored value[i] is the min-values of the according page[i+1} 
			//TODO implement binary search
			for (short i = 0; i < nEntries; i++) {
				if (keys[i]>key) {
					//TODO use weak refs
					//read page before that value
					return ((ULLIndexPage)readOrCreatePage(i)).locatePageForKey(key);
				}
			}
			//read last page
			return ((ULLIndexPage)readOrCreatePage(nEntries)).locatePageForKey(key);
		}
		
		public LLEntry getValueFromLeaf(long oid) {
			if (!isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			//TODO use better search alg
			for (int i = 0; i < nEntries; i++ ) {
				if (keys[i] == oid) {
					return new LLEntry( oid, values[i]);
				}
			}
			//TODO if non-unique, the value could be on the following page!
			return null;
		}

		public void put(long key, long value) {
			if (!isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			if (nEntries < maxLeafN) {
				//add locally
				//TODO use better search alg
				for (int i = 0; i < nEntries; i++ ) {
					if (keys[i] == key) {
						if (value != values[i]) {
							markDirty();
						}
						values[i] = value;
						return;
					} else if (keys[i] > key) {
						System.arraycopy(keys, i, keys, i+1, nEntries-i);
						System.arraycopy(values, i, values, i+1, nEntries-i);
						keys[i] = key;
						values[i] = value;
						nEntries++;
						
						markPageDirty();
						return;
					}
				}
				//append entry
				keys[nEntries] = key;
				values[nEntries] = value;
				nEntries++;
				
				markPageDirty();
				return;
			} else {
				//treat page overflow
				ULLIndexPage newP = new ULLIndexPage(root, true);
				System.arraycopy(keys, minLeafN, newP.keys, 0, maxLeafN-minLeafN);
				System.arraycopy(values, minLeafN, newP.values, 0, maxLeafN-minLeafN);
				nEntries = (short) minLeafN;
				newP.nEntries = (short) (maxLeafN-minLeafN);
				//New page and min key
				root.addLeafPage(newP, newP.keys[0], this);
				markPageDirty();
				newP.markPageDirty();
				if (newP.keys[0] > key) {
					put(key, value);
				} else {
					newP.put(key, value);
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
				ULLIndexPage newInner = createPage(root, false);
				
				//TODO use optimized fill ration for OIDS, just like above.
				System.arraycopy(keys, minInnerN+1, newInner.keys, 0, nEntries-minInnerN-1);
				System.arraycopy(leaves, minInnerN+1, newInner.leaves, 0, nEntries-minInnerN);
				System.arraycopy(leafPages, minInnerN+1, newInner.leafPages, 0, nEntries-minInnerN);
				newInner.nEntries = (short) (nEntries-minInnerN-1);
				newInner.updateLeafRoot();

				if (root == null) {
					ULLIndexPage newRoot = createPage(null, false);
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
						ULLIndexPage prevPage = (ULLIndexPage) root.getPrevLeafPage(this);
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
						ULLIndexPage prev = (ULLIndexPage) root.getPrevLeafPage(this);
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
							prev.keys[prev.nEntries] = ((ULLIndexPage)root).keys[pos]; 
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
					return Long.MIN_VALUE;
				}
				return keys[nEntries-1];
			}
			//handle empty indices
			if (nEntries == 0 && leaves[nEntries] == null && leafPages[nEntries] == 0) {
				return Long.MIN_VALUE;
			}
			long max = ((ULLIndexPage)getPageByPos(nEntries)).getMax();
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
	
	private transient ULLIndexPage root;
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public PagedUniqueLongLong(PageAccessFile raf) {
		super(raf, true, 8, 8);
		System.out.println("OidIndex entries per page: " + maxLeafN + " / inner: " + maxInnerN);
		//bootstrap index
		root = createPage(null, false);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedUniqueLongLong(PageAccessFile raf, int pageId) {
		super(raf, true, 8, 8);
		root = (ULLIndexPage) readRoot(pageId);
	}

	public void addLong(long key, long value) {
		ULLIndexPage page = getRoot().locatePageForKey(key);
		page.put(key, value);
	}

	public boolean removeLong(long key) {
		ULLIndexPage page = getRoot().locatePageForKey(key);
		return page.remove(key);
	}

	public LLEntry findValue(long key) {
		ULLIndexPage page = getRoot().locatePageForKey(key);
		return page.getValueFromLeaf(key);
	}

	@Override
	ULLIndexPage createPage(AbstractIndexPage parent, boolean isLeaf) {
		return new ULLIndexPage(parent, isLeaf);
	}

	@Override
	protected ULLIndexPage getRoot() {
		return root;
	}

	public Iterator<LLEntry> iterator(long min, long max) {
		return new ULLIterator(getRoot(), min, max);
	}

	@Override
	protected void updateRoot(AbstractIndexPage newRoot) {
		root = (ULLIndexPage) newRoot;
	}
	
	public void print() {
		root.print();
	}

	public long getMaxValue() {
		return root.getMax();
	}

	public Iterator<LLEntry> descendingIterator(long max, long min) {
		return new ULLDescendingIterator(getRoot(), max, min);
	}

}
