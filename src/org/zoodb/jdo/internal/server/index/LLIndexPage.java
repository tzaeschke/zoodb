package org.zoodb.jdo.internal.server.index;

import java.util.Arrays;
import java.util.NoSuchElementException;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

class LLIndexPage extends AbstractIndexPage {
	private LLIndexPage parent;
	private final long[] keys;
	private final long[] values;
	/** number of keys. There are nEntries+1 subPages in any leaf page. */
	private short nEntries;
	
	
	public LLIndexPage(AbstractPagedIndex ind, LLIndexPage parent, boolean isLeaf) {
		super(ind, parent, isLeaf);
		this.parent = parent;
		if (isLeaf) {
			nEntries = 0;
			keys = new long[ind.maxLeafN];
			values = new long[ind.maxLeafN];
		} else {
			nEntries = -1;
			keys = new long[ind.maxInnerN];
			if (ind.isUnique()) {
				values = null;
			} else {
				values = new long[ind.maxInnerN];
			}
		}
	}

	public LLIndexPage(LLIndexPage p) {
		super(p);
		keys = p.keys.clone();
		nEntries = p.nEntries;
		parent = p.parent;
		if (isLeaf) {
			values = p.values.clone();
		} else {
			if (ind.isUnique()) {
				values = null;
			} else {
				values = new long[ind.maxInnerN];
			}
		}
	}
	
	@Override
	void readData() {
		nEntries = ind.paf.readShort();
//		for (int i = 0; i < nEntries; i++) {
//			keys[i] = ind.paf.readLong();
//			values[i] = ind.paf.readLong();
//		}
		//TODO write only (nEntries) number of elements?
		ind.paf.noCheckRead(keys);
		switch (ind.valSize) {
		case 8: ind.paf.noCheckRead(values); break;
//		case 4: ind.paf.noCheckReadInt(values); break;
		//TODO remove this?
		case 0: for (int i = 0; i < nEntries; i++) values[i] = 0; break;
		default : throw new IllegalStateException("val-size=" + ind.valSize);
		}
	}
	
	@Override
	void writeData() {
		ind.paf.writeShort(nEntries);
//		for (int i = 0; i < nEntries; i++) {
//			ind.paf.writeLong(keys[i]);
//			ind.paf.writeLong(values[i]);
//		}
		ind.paf.noCheckWrite(keys);
		switch (ind.valSize) {
		case 8: ind.paf.noCheckWrite(values); break;
//		case 4: ind.paf.noCheckWriteInt(values); break;
		case 0: break;
		default : throw new IllegalStateException("val-size=" + ind.valSize);
		}
	}

	/**
	 * Locate the (first) page that could contain the given key.
	 * In the inner pages, the keys are the minimum values of the following page.
	 * @param key
	 * @return Page for that key
	 */
	public final LLIndexPage locatePageForKeyUnique(long key, boolean allowCreate) {
		if (isLeaf) {
			return this;
		}
		if (nEntries == -1 && !allowCreate) {
			return null;
		}
		
		//The stored value[i] is the min-values of the according page[i+1} 
        int pos = binarySearchUnique(0, nEntries, key);
        if (pos >= 0) {
            //pos of matching key
            pos++;
        } else {
            pos = -(pos+1);
        }
        //TODO use weak refs
        //read page before that value
        LLIndexPage page = (LLIndexPage) readOrCreatePage(pos, allowCreate);
        return page.locatePageForKeyUnique(key, allowCreate);
	}
	
	/**
	 * Locate the (first) page that could contain the given key.
	 * In the inner pages, the keys are the minimum values of the sub-page. The value is
	 * the according minimum value of the first key of the sub-page.
	 * @param key
	 * @return Page for that key
	 */
	public LLIndexPage locatePageForKey(long key, long value, boolean allowCreate) {
		if (isLeaf) {
			return this;
		}
		if (nEntries == -1 && !allowCreate) {
			return null;
		}

		//The stored value[i] is the min-values of the according page[i+1} 
        int pos = binarySearch(0, nEntries, key, value);
        if (pos >= 0) {
            //pos of matching key
            pos++;
        } else {
            pos = -(pos+1);
        }
        if (!ind.isUnique()) {
        	int keyPos = pos-1;
        	while (keyPos > 0 && keys[keyPos-1] == key && values[keyPos-1] <= value) {
        		keyPos--;
        	}
        	if (keyPos == 0 && keys[0] == key && values[0] > value) {
        		//becomes pos=0
        		keyPos--;
        	}
        	while (keyPos < nEntries-1 && keys[keyPos+1] == key && values[keyPos+1] <= value) {
        		keyPos++;
        	}
        	pos = keyPos+1;
        }
        //TODO use weak refs
        //read page before that value
        LLIndexPage page = (LLIndexPage) readOrCreatePage(pos, allowCreate);
        return page.locatePageForKey(key, value, allowCreate);
	}
	
	public LLEntry getValueFromLeafUnique(long oid) {
		if (!isLeaf) {
			throw new JDOFatalDataStoreException();
		}
		int pos = binarySearchUnique(0, nEntries, oid);
		if (pos >= 0) {
            return new LLEntry( oid, values[pos]);
		}
		//Even if non-unique, if the value could is not on this page, it does not exist.
		return null;
	}


    /**
     * Add an entry at 'key'/'value'. If the PAIR already exists, nothing happens.
     * @param key
     * @param value
     */
	public void insert(long key, long value) {
		put(key, value); 
	}

	/**
	 * Binary search.
	 * 
	 * @param toIndex Exclusive, search stops at (toIndex-1).
	 * @param value For non-unique trees, the value is taken into account as well.
	 */
	int binarySearch(int fromIndex, int toIndex, long key, long value) {
		if (ind.isUnique()) {
			return binarySearchUnique(fromIndex, toIndex, key);
		}
		return binarySearchNonUnique(fromIndex, toIndex, key, value);
	}
	
	private int binarySearchUnique(int fromIndex, int toIndex, long key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
        	long midVal = keys[mid];

        	if (midVal < key)
        		low = mid + 1;
        	else if (midVal > key)
        		high = mid - 1;
        	else {
       			return mid; // key found
        	}
		}
		return -(low + 1);  // key not found.
	}

	/**
	 * This effectively implements a binary search of a double-long array (128bit values).
	 */
	private int binarySearchNonUnique(int fromIndex, int toIndex, long key1, long key2) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			long midVal1 = keys[mid];
			long midVal2 = values[mid];

        	if (midVal1 < key1) {
        		low = mid + 1;
        	} else if (midVal1 > key1) {
        		high = mid - 1;
        	} else {
            	if (midVal2 < key2) {
            		low = mid + 1;
            	} else if (midVal2 > key2) {
            		high = mid - 1;
            	} else {
            		return mid; // key found
            	}
        	}
		}
		return -(low + 1);  // key not found.
	}

	
    /**
     * Overwrite the entry at 'key'.
     * @param key
     * @param value
     */
	public final void put(long key, long value) {
		if (!isLeaf) {
			throw new JDOFatalDataStoreException();
		}

		//in any case, check whether the key(+value) already exists
        int pos = binarySearch(0, nEntries, key, value);
        //key found? -> pos >=0
        if (pos >= 0) {
        	//for unique entries
            if (value != values[pos]) {
                markPageDirtyAndClone();
                values[pos] = value;
            }
            return;
        } 

        if (nEntries < ind.maxLeafN) {
            //okay so we add it locally
            pos = -(pos+1);
            markPageDirtyAndClone();
            if (pos < nEntries) {
                System.arraycopy(keys, pos, keys, pos+1, nEntries-pos);
                System.arraycopy(values, pos, values, pos+1, nEntries-pos);
            }
            keys[pos] = key;
            values[pos] = value;
            nEntries++;
            return;
		} else {
			//treat page overflow
			
			//TODO check neighboring pages for space
			
			LLIndexPage newP = new LLIndexPage(ind, parent, true);
			markPageDirtyAndClone();
			int nEntriesToKeep = ind.minLeafN;
			int nEntriesToCopy = ind.maxLeafN - ind.minLeafN;
			if (ind.isUnique()) {
				//find split point such that pages can be completely full
	            int pos2 = binarySearch(0, nEntries, keys[0] + ind.maxLeafN, value);
	            if (pos2 < 0) {
	                pos2 = -(pos2+1);
	            }
	            if (pos2 > nEntriesToKeep) {
	            	nEntriesToKeep = pos2;
	            	nEntriesToCopy = ind.maxLeafN - nEntriesToKeep;
	            }
			}
			System.arraycopy(keys, nEntriesToKeep, newP.keys, 0, nEntriesToCopy);
			System.arraycopy(values, nEntriesToKeep, newP.values, 0, nEntriesToCopy);
			nEntries = (short) nEntriesToKeep;
			newP.nEntries = (short) (nEntriesToCopy);
			//New page and min key
			if (ind.isUnique()) {
				if (newP.keys[0] >= key) {
					put(key, value);
				} else {
					newP.put(key, value);
				}
				parent.addSubPage(newP, newP.keys[0], newP.values[0]);
			} else {
				//why doesn't this work??? Because addSubPage needs the new keys already in the 
				//page
//				parent.addSubPage(newP, newP.keys[0], newP.values[0]);
//				locatePageForKey(key, value, false).put(key, value);
				if (newP.keys[0] > key || newP.keys[0]==key && newP.values[0] > value) {
					put(key, value);
				} else {
					newP.put(key, value);
				}
				parent.addSubPage(newP, newP.keys[0], newP.values[0]);				
			}
		}
	}

	void addSubPage(LLIndexPage newP, long minKey, long minValue) {
		if (isLeaf) {
			throw new JDOFatalDataStoreException();
		}
		
		markPageDirtyAndClone();
		if (nEntries < ind.maxInnerN) {
			//add page here
			//TODO Should we store non-unique values more efficiently? Instead of always storing
			//     the key as well? -> Additional page type for values only? The local value only
			//     being a reference to the value page (inlc offs)? How would efficient insertion
			//     work (shifting values and references to value pages???) ?
			
			
			//For now, we assume a unique index.
            int i = binarySearch(0, nEntries, minKey, minValue);
            //If the key (+val) has a perfect match then something went wrong. This should
            //never happen so we don't need to check whether (i < 0).
        	i = -(i+1);
            
			if (i > 0) {
				System.arraycopy(keys, i, keys, i+1, nEntries-i);
				System.arraycopy(leaves, i+1, leaves, i+2, nEntries-i);
				System.arraycopy(leafPages, i+1, leafPages, i+2, nEntries-i);
				if (!ind.isUnique()) {
					System.arraycopy(values, i, values, i+1, nEntries-i);
					values[i] = minValue;
				}
				keys[i] = minKey;
				leaves[i+1] = newP;
				newP.setParent( this );
				leafPages[i+1] = 0;
				nEntries++;
			} else {
				//decide whether before or after first page (both will end up before the current
				//first key).
				int ii;
				if (nEntries < 0) {
					//can happen for empty root page
					ii = 0;
				} else {
					System.arraycopy(keys, 0, keys, 1, nEntries);
					long oldKey = leaves[0].getMinKey();
					if (!ind.isUnique()) {
						System.arraycopy(values, 0, values, 1, nEntries);
						long oldValue = leaves[0].getMinKeyValue();
						if ((minKey > oldKey) || (minKey==oldKey && minValue > oldValue)) {
							ii = 1;
							keys[0] = minKey;
							values[0] = minValue;
						} else {
							ii = 0;
							keys[0] = oldKey;
							values[0] = oldValue;
						}
					} else {
						if ( minKey > oldKey ) {
							ii = 1;
							keys[0] = minKey;
						} else {
							ii = 0;
							keys[0] = oldKey;
						}
					}
					System.arraycopy(leaves, ii, leaves, ii+1, nEntries-ii+1);
					System.arraycopy(leafPages, ii, leafPages, ii+1, nEntries-ii+1);
				}
				leaves[ii] = newP;
				newP.setParent( this );
				leafPages[ii] = 0;
				nEntries++;
			}
			return;
		} else {
			//treat page overflow
			LLIndexPage newInner = (LLIndexPage) ind.createPage(parent, false);
			
			//TODO use optimized fill ration for unique values, just like for leaves?.
			System.arraycopy(keys, ind.minInnerN+1, newInner.keys, 0, nEntries-ind.minInnerN-1);
			if (!ind.isUnique()) {
				System.arraycopy(values, ind.minInnerN+1, newInner.values, 0, nEntries-ind.minInnerN-1);
			}
			System.arraycopy(leaves, ind.minInnerN+1, newInner.leaves, 0, nEntries-ind.minInnerN);
			System.arraycopy(leafPages, ind.minInnerN+1, newInner.leafPages, 0, nEntries-ind.minInnerN);
			newInner.nEntries = (short) (nEntries-ind.minInnerN-1);
			newInner.assignThisAsRootToLeaves();

			if (parent == null) {
				//create a parent
				LLIndexPage newRoot = (LLIndexPage) ind.createPage(null, false);
				newRoot.leaves[0] = this;
				newRoot.nEntries = 0;  // 0: indicates one leaf / zero keys
				this.setParent( newRoot );
				ind.updateRoot(newRoot);
			}

			if (ind.isUnique()) {
				parent.addSubPage(newInner, keys[ind.minInnerN], -1);
			} else {
				parent.addSubPage(newInner, keys[ind.minInnerN], values[ind.minInnerN]);
			}

			nEntries = (short) (ind.minInnerN);
			//finally add the leaf to the according page
			LLIndexPage newHome;
			long newInnerMinKey = newInner.getMinKey();
			if (ind.isUnique()) {
				if (minKey < newInnerMinKey) {
					newHome = this;
				} else {
					newHome = newInner;
				}
			} else {
				long newInnerMinValue = newInner.getMinKeyValue();
				if (minKey < newInnerMinKey || 
						(minKey == newInnerMinKey && minValue < newInnerMinValue)) {
					newHome = this;
				} else {
					newHome = newInner;
				}
			}
			newHome.addSubPage(newP, minKey, minValue);
//			if (minKey < newInner.keys[0]) {
//				newHome = this;
//				addSubPage(newP, minKey, minValue);
//			} else {
//				newInner.addSubPage(newP, minKey, minValue);
//			}
			return;
		}
	}
	
	@Override
	long getMinKey() {
		if (isLeaf) {
			return keys[0];
		}
		return readPage(0).getMinKey();
	}
	
	@Override
	long getMinKeyValue() {
		if (isLeaf) {
			return values[0];
		}
		return readPage(0).getMinKeyValue();
	}
	
	public void print(String indent) {
//		System.out.println("Java page ID: " + this);  //TODO
		if (isLeaf) {
			System.out.println(indent + "Leaf page(id=" + pageId() + "): nK=" + nEntries + " keys=" + 
					Arrays.toString(keys));
			System.out.println(indent + "                         " + Arrays.toString(values));
		} else {
			System.out.println(indent + "Inner page(id=" + pageId() + "): nK=" + nEntries + " keys=" + 
					Arrays.toString(keys));
			System.out.println(indent + "                " + nEntries + " page=" + 
					Arrays.toString(leafPages));
			if (!ind.isUnique()) {
				System.out.println(indent + "              " + nEntries + " values=" + 
						Arrays.toString(values));
			}
//			System.out.println(indent + "                " + nEntries + " leaf=" + 
//					Arrays.toString(leaves));
			System.out.print(indent + "[");
			for (int i = 0; i <= nEntries; i++) {
				if (leaves[i] != null) { 
					System.out.print(indent + "i=" + i + ": ");
					leaves[i].print(indent + "  ");
				}
				else System.out.println("Page not loaded: " + leafPages[i]);
			}
			System.out.println(']');
		}
	}

	public void printLocal() {
		System.out.println("PrintLocal() for " + this);
		if (isLeaf) {
			System.out.println("Leaf page(id=" + pageId() + "): nK=" + nEntries + " oids=" + 
					Arrays.toString(keys));
			System.out.println("                         " + Arrays.toString(values));
		} else {
			System.out.println("Inner page(id=" + pageId() + "): nK=" + nEntries + " oids=" + 
					Arrays.toString(keys));
			System.out.println("                      " + Arrays.toString(leafPages));
			if (!ind.isUnique()) {
				System.out.println("                      " + Arrays.toString(values));
			}
			System.out.println("                      " + Arrays.toString(leaves));
		}
	}

	protected short getNEntries() {
		return nEntries;
	}
	
	/**
	 * @param key
	 * @return the previous value
	 */
	protected long remove(long key) {
		if (!ind.isUnique()) {
			throw new IllegalStateException();
		}
		return remove(key, 0);
	}
	
	protected long remove(long oid, long value) {
        int i = binarySearch(0, nEntries, oid, value);
        if (i < 0) {
        	//key not found
        	throw new NoSuchElementException("Key not found: " + oid + "/" + value);
        }
        
        // first remove the element
        markPageDirtyAndClone();
        long prevValue = values[i];
        System.arraycopy(keys, i+1, keys, i, nEntries-i-1);
        System.arraycopy(values, i+1, values, i, nEntries-i-1);
        nEntries--;
        if (nEntries == 0) {
        	ind.statNLeaves--;
        	parent.removeLeafPage(this, oid, value);
        } else if (nEntries < (ind.maxLeafN >> 1) && (nEntries % 8 == 0)) {
        	//The second term prevents frequent reading of previous and following pages.
        	//TODO Should we instead check for nEntries==MAx>>1 then == (MAX>>2) then <= (MAX>>3)?

        	//now attempt merging this page
        	LLIndexPage prevPage = (LLIndexPage) parent.getPrevLeafPage(this);
        	if (prevPage != null) {
         		//We merge only if they all fit on a single page. This means we may read
        		//the previous page unnecessarily, but we avoid writing it as long as 
        		//possible. TODO find a balance, and do no read prev page in all cases
        		if (nEntries + prevPage.nEntries < ind.maxLeafN) {
        			//TODO for now this work only for leaves with the same root. We
        			//would need to update the min values in the inner nodes.
        			prevPage.markPageDirtyAndClone();
        			System.arraycopy(keys, 0, prevPage.keys, prevPage.nEntries, nEntries);
        			System.arraycopy(values, 0, prevPage.values, prevPage.nEntries, nEntries);
        			prevPage.nEntries += nEntries;
        			ind.statNLeaves--;
        			parent.removeLeafPage(this, keys[0], values[0]);
        		}
        	}
        }
        return prevValue;
	}


	protected void removeLeafPage(LLIndexPage indexPage, long key, long value) {
		ind.statNInner--;

		int start = binarySearch(0, nEntries, key, value);
		if (start < 0) {
			start = -(start+1);
		}
		
		for (int i = start; i <= nEntries; i++) {
			if (leaves[i] == indexPage) {
				//remove page from FSM.
				ind._raf.releasePage(leafPages[i]);
				if (nEntries > 0) { //otherwise we just delete this page
				    //removeLeafPage() is only called by leaves that have already called markPageDirty().
					markPageDirtyAndClone();
					if (i < nEntries) {  //otherwise it's the last element
						arraysRemoveInnerEntry(i);
					} else {
						nEntries--;
					}
				
					//Now try merging
					if (parent == null) {
						return;
					}
					LLIndexPage prev = (LLIndexPage) parent.getPrevLeafPage(this);
					if (prev != null && !prev.isLeaf) {
						//TODO this is only good for merging inside the same root.
						if ((nEntries % 2 == 0) && (prev.nEntries + nEntries < ind.maxInnerN)) {
						    prev.markPageDirtyAndClone();
							System.arraycopy(keys, 0, prev.keys, prev.nEntries+1, nEntries);
							if (!ind.isUnique()) {
								System.arraycopy(values, 0, prev.values, prev.nEntries+1, nEntries);
							}
							System.arraycopy(leaves, 0, prev.leaves, prev.nEntries+1, nEntries+1);
							System.arraycopy(leafPages, 0, prev.leafPages, prev.nEntries+1, nEntries+1);
							//find key for the first appended page -> go up or go down????? Up!
							int pos = parent.getPagePosition(this)-1;
							prev.keys[prev.nEntries] = parent.keys[pos]; 
							if (!ind.isUnique()) {
								prev.values[prev.nEntries] = parent.values[pos]; 
							}
							prev.nEntries += nEntries + 1;  //for the additional key
							prev.assignThisAsRootToLeaves();
							parent.removeLeafPage(this, key, value);
						}
						return;
					}
				} else if (parent != null) {
					//TODO set check above to allow 1
//					if (nEntries == 1) {
//						parent.replaceChildPage(this, key, value, leaves[1], leafPages[1]);
//					} else {
//						//TODO
//						System.out.println("1--hfdakshfkldhsafhkldhlkf");
//						//TODO can this happen? I think not! remove!
//						parent.removeLeafPage(this, key, value);
//						nEntries--;
//					}
					parent.removeLeafPage(this, key, value);
					nEntries--;
				} else {
					//TODO set check above to allow 1
//					if (nEntries == 1) { //otherwise we just delete this page
//					    //removeLeafPage() is only called by leaves that have already called markPageDirty().
//						markPageDirtyAndClone();
////						if (i < nEntries) {  //otherwise it's the last element
//					if (i > 0) {
//						arraysRemoveKey(i-1);
//					} else {
//						arraysRemoveKey(0);
//					}
//					arraysRemoveChild(i);
//						}
//						nEntries--;
//						return;
//					}
//					//TODO
//					System.out.println("2--hfdakshfkldhsafhkldhlkf");
					//No root and this is a leaf page... -> we do nothing.
					leafPages[0] = 0;
					leaves[0] = null;
					nEntries = -1;
				}
				return;
			}
		}
		System.out.println("this:" + parent);
		this.printLocal();
		System.out.println("leaf: " + indexPage);
		indexPage.printLocal();
		throw new JDOFatalDataStoreException("leaf page not found.");
	}

	private void arraysRemoveKey(int pos) {
		System.arraycopy(keys, pos+1, keys, pos, nEntries-pos-1);
		if (!ind.isUnique()) {
			System.arraycopy(values, pos+1, values, pos, nEntries-pos-1);
		}
	}
	
	private void arraysRemoveChild(int pos) {
		System.arraycopy(leaves, pos+1, leaves, pos, nEntries-pos);
		System.arraycopy(leafPages, pos+1, leafPages, pos, nEntries-pos);
		leafPages[nEntries] = 0;
		leaves[nEntries] = null;
	}
	
	/**
	 * 
	 * @param posEntry The pos in the child-array. The according keyPos may be -1.
	 */
	private void arraysRemoveInnerEntry(int posEntry) {
		if (posEntry > 0) {
			arraysRemoveKey(posEntry - 1);
		} else {
			arraysRemoveKey(0);
		}
		arraysRemoveChild(posEntry);
		nEntries--;
	}

	private void arraysAppendInner(LLIndexPage src) {
		System.arraycopy(src.keys, 0, keys, nEntries+1, src.nEntries);
		if (!ind.isUnique()) {
			System.arraycopy(src.values, 0, values, nEntries+1, src.nEntries);
		}
		System.arraycopy(src.leaves, 0, leaves, nEntries+1, src.nEntries+1);
		System.arraycopy(src.leafPages, 0, leafPages, nEntries+1, src.nEntries+1);
		//inc nEntries?
		assignThisAsRootToLeaves();
	}
	
	
	/**
	 * Replacing child pages occurs when the child shrinks down to a single sub-child, in which
	 * case we pull up the sub-child to the local page, replacing the child.
	 */
	protected void replaceChildPage(LLIndexPage indexPage, long key, long value, 
			AbstractIndexPage subChild, int subChildPageId) {
		int start = binarySearch(0, nEntries, key, value);
		if (start < 0) {
			start = -(start+1);
		}
		for (int i = start; i <= nEntries; i++) {
			if (leaves[i] == indexPage) {
				System.out.println("REMOVING PAGE: " + i);
				System.out.println("parent before");
				printLocal();
				System.out.println("child");
				indexPage.printLocal();
				System.out.println("subChild");
				subChild.printLocal();
				
				//remove page from FSM.
				ind._raf.releasePage(leafPages[i]);
				leafPages[i] = subChildPageId;
				leaves[i] = subChild;
				if (i>0) {
					keys[i-1] = key;
					if (!ind.isUnique()) {
						values[i-1] = value;
					}
				}
				subChild.setParent(this);

				System.out.println("parent after");
				printLocal();
				System.out.println("DONE");

				//TODO???
				//removeLeafPage() is only called by leaves that have already called markPageDirty().
				markPageDirtyAndClone();
				return;
			}
		}
		System.out.println("this:" + parent);
		this.printLocal();
		System.out.println("child:");
		indexPage.printLocal();
		throw new JDOFatalDataStoreException("child page not found.");
	}

	@Override
	LLIndexPage getParent() {
		return parent;
	}
	
	@Override
	void setParent(AbstractIndexPage parent) {
		this.parent = (LLIndexPage) parent;
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
		long max = ((LLIndexPage)getPageByPos(nEntries)).getMax();
		return max;
	}

	@Override
	void writeKeys() {
		ind._raf.writeShort(nEntries);
		ind._raf.noCheckWrite(keys);
		if (!ind.isUnique()) {
			ind._raf.noCheckWrite(values);
		}
	}

	@Override
	void readKeys() {
		nEntries = ind._raf.readShort();
		ind._raf.noCheckRead(keys);
		if (!ind.isUnique()) {
			ind._raf.noCheckRead(values);
		}
	}

	@Override
	protected AbstractIndexPage newInstance() {
		return new LLIndexPage(this);
	}

	/**
	 * Checks for entries in the given range.
	 * @param min
	 * @param max
	 * @return 0: no entries in index; 1: entries found; 2: entries may be on earlier page;
	 * 3: entries may be on later page.
	 */
	public int containsEntryInRangeUnique(long min, long max) {
		int posMin = binarySearchUnique(0, nEntries, min);
		if (posMin >= 0) {
			return 1;
		}
		posMin = (short) -(posMin+1);
        if (posMin == nEntries) {
            //not on this page, but may be on next
            return 3;
        }
        
        if (keys[posMin] > max && posMin != 0) {
            //In theory this could be wrong for posMin==0, but that would mean that
            //we are on a completely wrong page already.
            return 0;
        }

        return 4;
        
//        
//        if (posMin == 0) {
//            if (keys[0] > max) {
//                //not on this page, but may be on previous
//                return 2;
//            } else if (keys[0] < max) {
//                //not on this page, but may be on previous
//                return 1;
//            } else {
//                //keys[0] == max;
//                return 1;
//            }
//        }
//        
//        //TODO we also need to cover the case where nEntires = 0 ?
//        
//        //return true;
//        return (keys[pos] <= max);
	}

	@Override
	protected void incrementNEntries() {
		nEntries++;
	}

	final long[] getKeys() {
		return keys;
	}

	final long[] getValues() {
		return values;
	}
}
