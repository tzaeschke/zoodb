/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal.server.index;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.util.DBLogger;

class LLIndexPage extends AbstractIndexPage {
	private LLIndexPage parent;
	private final long[] keys;
	private final long[] values;
	/** number of keys. There are nEntries+1 subPages in any leaf page. */
	private short nEntries;
	
	
	public LLIndexPage(AbstractPagedIndex ind, LLIndexPage parent, boolean isLeaf) {
		super(ind, isLeaf);
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
				values = p.values.clone();
			}
		}
	}
	
	@Override
	void readData(StorageChannelInput in) {
		nEntries = in.readShort();
		readArrayFromRaf(in, ind.keySize, keys, nEntries);
		readArrayFromRaf(in, ind.valSize, values, nEntries);
	}
	
	@Override
	void writeData(StorageChannelOutput out) {
		out.writeShort(nEntries);
		writeArrayToRaf(out, ind.keySize, keys, nEntries);
		writeArrayToRaf(out, ind.valSize, values, nEntries);
	}

	@Override
	void writeKeys(StorageChannelOutput out) {
		out.writeShort(nEntries);
		writeArrayToRaf(out, ind.keySize, keys, nEntries);
		if (!ind.isUnique()) {
			writeArrayToRaf(out, ind.valSize, values, nEntries);
		}
	}

	@Override
	void readKeys(StorageChannelInput in) {
		nEntries = in.readShort();
		readArrayFromRaf(in, ind.keySize, keys, nEntries);
		if (!ind.isUnique()) {
			readArrayFromRaf(in, ind.valSize, values, nEntries);
		}
	}
	
	private static void writeArrayToRaf(StorageChannelOutput out, int bitWidth, long[] array, int nEntries) {
		if (nEntries <= 0) {
			return;
		}
		switch (bitWidth) {
		case 8: out.noCheckWrite(array); break;
//		case 8:
//			//writing ints using a normal loop
//			for (int i = 0; i < nEntries; i++) {
//				ind.paf.writeLong(array[i]); 
//			}
//			break;
		case 4:
			out.noCheckWriteAsInt(array, nEntries); break;
		case 1:
			//writing bytes using an array (different to int-write, see PerfByteArrayRW)
			byte[] ba = new byte[nEntries];
			for (int i = 0; i < ba.length; i++) {
				ba[i] = (byte) array[i];
			}
			out.noCheckWrite(ba); 
			break;
		case 0: break;
		default : throw new IllegalStateException("bit-width=" + bitWidth);
		}
	}

	private static void readArrayFromRaf(StorageChannelInput in, int bitWidth, long[] array, int nEntries) {
		if (nEntries <= 0) {
			return;
		}
		switch (bitWidth) {
		case 8: in.noCheckRead(array); break;
//		case 8:
//			//reading ints using a normal loop
//			for (int i = 0; i < nEntries; i++) {
//				array[i] = ind.paf.readLong();
//			}
//			break;
		case 4:
			in.noCheckReadAsInt(array, nEntries); break;
		case 1:
			//reading bytes using an array (different to int-write, see PerfByteArrayRW)
			byte[] ba = new byte[nEntries];
			in.noCheckRead(ba); 
			for (int i = 0; i < ba.length; i++) {
				array[i] = ba[i];
			}
			break;
		case 0: break;
		default : throw new IllegalStateException("bit-width=" + bitWidth);
		}
	}
	
	
	/**
	 * Locate the (first) page that could contain the given key.
	 * In the inner pages, the keys are the minimum values of the following page.
	 * @param key
	 * @return Page for that key
	 */
	public final LLIndexPage locatePageForKeyUnique(long key, boolean allowCreate) {
		return locatePageForKey(key, -1, allowCreate);
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
        //TODO use weak refs
        //read page before that value
        LLIndexPage page = (LLIndexPage) readOrCreatePage(pos, allowCreate);
        return page.locatePageForKey(key, value, allowCreate);
	}
	
	public LongLongIndex.LLEntry getValueFromLeafUnique(long oid) {
		if (!isLeaf) {
			throw DBLogger.newFatalInternal("Leaf inconsistency.");
		}
		int pos = binarySearchUnique(0, nEntries, oid);
		if (pos >= 0) {
            return new LongLongIndex.LLEntry( oid, values[pos]);
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
			throw DBLogger.newFatalInternal("Tree inconsistency.");
		}

		//in any case, check whether the key(+value) already exists
        int pos = binarySearch(0, nEntries, key, value);
        //key found? -> pos >=0
        if (pos >= 0) {
        	//check if values changes
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
			LLIndexPage newP;
			boolean isNew = false;
			boolean isPrev = false;
			//use ind.maxLeafN -1 to avoid pretty much pointless copying (and possible endless 
			//loops, see iterator tests)
			LLIndexPage next = (LLIndexPage) parent.getNextLeafPage(this);
			if (next != null && next.nEntries < ind.maxLeafN-1) {
				//merge
				newP = next;
				newP.markPageDirtyAndClone();
				isPrev = false;
			} else {
				//Merging with prev is not make a big difference, maybe we should remove it...
				LLIndexPage prev = (LLIndexPage) parent.getPrevLeafPage(this);
				if (prev != null && prev.nEntries < ind.maxLeafN-1) {
					//merge
					newP = prev;
					newP.markPageDirtyAndClone();
					isPrev = true;
				} else {
					newP = new LLIndexPage(ind, parent, true);
					isNew = true;
				}
			}
			
			markPageDirtyAndClone();
			int nEntriesToKeep = (nEntries + newP.nEntries) >> 1;
			if (isNew) {
				if (ind.isUnique()) {
					//This is an optimization for indices that add increasing unique numbers 
					//such as OIDs. For these, it increases the average fill-size.
					//find split point such that pages can be completely full
					int pos2 = binarySearch(0, nEntries, keys[0] + ind.maxLeafN, value);
					if (pos2 < 0) {
						pos2 = -(pos2+1);
					}
					if (pos2 > nEntriesToKeep) {
						nEntriesToKeep = pos2;
					}
				} else {
					//non-unique: we assume ascending keys.
					//If they are not ascending, merging with subsequent page sorts it out.
					nEntriesToKeep = (int) (ind.maxLeafN * 0.9);
				}
			}
			int nEntriesToCopy = nEntries - nEntriesToKeep;
			if (isNew) {
				//works only if new page follows current page
				System.arraycopy(keys, nEntriesToKeep, newP.keys, 0, nEntriesToCopy);
				System.arraycopy(values, nEntriesToKeep, newP.values, 0, nEntriesToCopy);
			} else if (isPrev) {
				//copy element to previous page
				System.arraycopy(keys, 0, newP.keys, newP.nEntries, nEntriesToCopy);
				System.arraycopy(values, 0, newP.values, newP.nEntries, nEntriesToCopy);
				//move element forward to beginning of page
				System.arraycopy(keys, nEntriesToCopy, keys, 0, nEntries-nEntriesToCopy);
				System.arraycopy(values, nEntriesToCopy, values, 0, nEntries-nEntriesToCopy);
			} else {
				//make space on next page
				System.arraycopy(newP.keys, 0, newP.keys, nEntriesToCopy, newP.nEntries);
				System.arraycopy(newP.values, 0, newP.values, nEntriesToCopy, newP.nEntries);
				//insert element in next page
				System.arraycopy(keys, nEntriesToKeep, newP.keys, 0, nEntriesToCopy);
				System.arraycopy(values, nEntriesToKeep, newP.values, 0, nEntriesToCopy);
			}
			nEntries = (short) nEntriesToKeep;
			newP.nEntries = (short) (nEntriesToCopy + newP.nEntries);
			//New page and min key
			if (isNew || !isPrev) {
				if (ind.isUnique()) {
					if (newP.keys[0] > key) {
						put(key, value);
					} else {
						newP.put(key, value);
					}
				} else {
					//why doesn't this work??? Because addSubPage needs the new keys already in the 
					//page
	//				parent.addSubPage(newP, newP.keys[0], newP.values[0]);
	//				locatePageForKey(key, value, false).put(key, value);
					if (newP.keys[0] > key || (newP.keys[0]==key && newP.values[0] > value)) {
						put(key, value);
					} else {
						newP.put(key, value);
					}
				}
			} else {
				if (ind.isUnique()) {
					if (keys[0] > key) {
						newP.put(key, value);
					} else {
						put(key, value);
					}
				} else {
					if (keys[0] > key || (keys[0]==key && values[0] > value)) {
						newP.put(key, value);
					} else {
						put(key, value);
					}
				}
			}
			if (isNew) {
				parent.addSubPage(newP, newP.keys[0], newP.values[0]);
			} else {
				//TODO probably not necessary
				newP.parent.updateKey(newP, newP.keys[0], newP.values[0]);
			}
			parent.updateKey(this, keys[0], values[0]);
		}
	}

	void updateKey(LLIndexPage indexPage, long key, long value) {
		//TODO do we need this whole key update business????
		//-> surely not at the moment, where we only merge with pages that have the same 
		//   immediate parent...
		if (isLeaf) {
			throw DBLogger.newFatalInternal("Tree inconsistency");
		}
		int start = binarySearch(0, nEntries, key, value);
		if (start < 0) {
			start = -(start+1);
		}
		
		markPageDirtyAndClone();
		for (int i = start; i <= nEntries; i++) {
			if (subPages[i] == indexPage) {
				if (i > 0) {
					keys[i-1] = key;
					if (!ind.isUnique()) {
						values[i-1] = value;
					}
				} else {
					//parent page could be affected
					if (parent != null) {
						parent.updateKey(this, key, value);
					}
				}
				return;
			}
		}
//		System.out.println("this:" + parent);
//		this.printLocal();
//		System.out.println("leaf: " + indexPage);
//		indexPage.printLocal();
		throw DBLogger.newFatalInternal("leaf page not found.");
		
	}
	
	void addSubPage(LLIndexPage newP, long minKey, long minValue) {
		if (isLeaf) {
			throw DBLogger.newFatalInternal("Tree inconsistency");
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
				System.arraycopy(subPages, i+1, subPages, i+2, nEntries-i);
				System.arraycopy(subPageIds, i+1, subPageIds, i+2, nEntries-i);
				if (!ind.isUnique()) {
					System.arraycopy(values, i, values, i+1, nEntries-i);
					values[i] = minValue;
				}
				keys[i] = minKey;
				subPages[i+1] = newP;
				newP.setParent( this );
				subPageIds[i+1] = newP.pageId();
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
					long oldKey = subPages[0].getMinKey();
					if (!ind.isUnique()) {
						System.arraycopy(values, 0, values, 1, nEntries);
						long oldValue = subPages[0].getMinKeyValue();
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
					System.arraycopy(subPages, ii, subPages, ii+1, nEntries-ii+1);
					System.arraycopy(subPageIds, ii, subPageIds, ii+1, nEntries-ii+1);
				}
				subPages[ii] = newP;
				newP.setParent( this );
				subPageIds[ii] = newP.pageId();
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
			System.arraycopy(subPages, ind.minInnerN+1, newInner.subPages, 0, nEntries-ind.minInnerN);
			System.arraycopy(subPageIds, ind.minInnerN+1, newInner.subPageIds, 0, nEntries-ind.minInnerN);
			newInner.nEntries = (short) (nEntries-ind.minInnerN-1);
			newInner.assignThisAsRootToLeaves();

			if (parent == null) {
				//create a parent
				LLIndexPage newRoot = (LLIndexPage) ind.createPage(null, false);
				newRoot.subPages[0] = this;
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
	
	@Override
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
					Arrays.toString(subPageIds));
			if (!ind.isUnique()) {
				System.out.println(indent + "              " + nEntries + " values=" + 
						Arrays.toString(values));
			}
//			System.out.println(indent + "                " + nEntries + " leaf=" + 
//					Arrays.toString(leaves));
			System.out.print(indent + "[");
			for (int i = 0; i <= nEntries; i++) {
				if (subPages[i] != null) { 
					System.out.print(indent + "i=" + i + ": ");
					subPages[i].print(indent + "  ");
				}
				else System.out.println("Page not loaded: " + subPageIds[i]);
			}
			System.out.println(']');
		}
	}

	@Override
	public void printLocal() {
		System.out.println("PrintLocal() for " + this);
		if (isLeaf) {
			System.out.println("Leaf page(id=" + pageId() + "): nK=" + nEntries + " oids=" + 
					Arrays.toString(keys));
			System.out.println("                         " + Arrays.toString(values));
		} else {
			System.out.println("Inner page(id=" + pageId() + "): nK=" + nEntries + " oids=" + 
					Arrays.toString(keys));
			System.out.println("                      " + Arrays.toString(subPageIds));
			if (!ind.isUnique()) {
				System.out.println("                      " + Arrays.toString(values));
			}
			System.out.println("                      " + Arrays.toString(subPages));
		}
	}

	@Override
	protected short getNKeys() {
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
		int start = binarySearch(0, nEntries, key, value);
		if (start < 0) {
			start = -(start+1);
		}
		
		for (int i = start; i <= nEntries; i++) {
			if (subPages[i] == indexPage) {
				markPageDirtyAndClone();
				//remove sub page page from FSM.
				ind.file.reportFreePage(subPageIds[i]);

				if (nEntries > 0) { //otherwise we just delete this page
					//remove entry
					arraysRemoveInnerEntry(i);
					nEntries--;
				
					//Now try merging
					if (parent == null) {
						return;
					}
					LLIndexPage prev = (LLIndexPage) parent.getPrevInnerPage(this);
					if (prev != null && !prev.isLeaf) {
						//TODO this is only good for merging inside the same root.
						if ((nEntries % 2 == 0) && (prev.nEntries + nEntries < ind.maxInnerN)) {
						    prev.markPageDirtyAndClone();
							System.arraycopy(keys, 0, prev.keys, prev.nEntries+1, nEntries);
							if (!ind.isUnique()) {
								System.arraycopy(values, 0, prev.values, prev.nEntries+1, nEntries);
							}
							System.arraycopy(subPages, 0, prev.subPages, prev.nEntries+1, nEntries+1);
							System.arraycopy(subPageIds, 0, prev.subPageIds, prev.nEntries+1, nEntries+1);
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
					
					if (nEntries == 0) {
						//only one element left, no merging occurred -> move sub-page up to parent
						AbstractIndexPage child = readPage(0);
						parent.replaceChildPage(this, key, value, child);
						ind.statNInner--;
					}
				} else {
					// nEntries == 0
					if (parent != null) {
						parent.removeLeafPage(this, key, value);
						ind.statNInner--;
					}
					// else : No root and this is a leaf page... -> we do nothing.
					subPageIds[0] = 0;
					subPages[0] = null;
					nEntries--;  //down to -1 which indicates an empty root page
				}
				return;
			}
		}
//		System.out.println("this:" + parent);
//		this.printLocal();
//		System.out.println("leaf: " + indexPage);
//		indexPage.printLocal();
		throw DBLogger.newFatalInternal("leaf page not found.");
	}

	private void arraysRemoveKey(int pos) {
		System.arraycopy(keys, pos+1, keys, pos, nEntries-pos-1);
		if (!ind.isUnique()) {
			System.arraycopy(values, pos+1, values, pos, nEntries-pos-1);
		}
	}
	
	private void arraysRemoveChild(int pos) {
		System.arraycopy(subPages, pos+1, subPages, pos, nEntries-pos);
		System.arraycopy(subPageIds, pos+1, subPageIds, pos, nEntries-pos);
		subPageIds[nEntries] = 0;
		subPages[nEntries] = null;
	}
	
	/**
	 * 
	 * @param posEntry The pos in the subPage-array. The according keyPos may be -1.
	 */
	private void arraysRemoveInnerEntry(int posEntry) {
		if (posEntry > 0) {
			arraysRemoveKey(posEntry - 1);
		} else {
			arraysRemoveKey(0);
		}
		arraysRemoveChild(posEntry);
	}

	/**
	 * Replacing sub-pages occurs when the sub-page shrinks down to a single sub-sub-page, in which
	 * case we pull up the sub-sub-page to the local page, replacing the sub-page.
	 */
	protected void replaceChildPage(LLIndexPage indexPage, long key, long value, 
			AbstractIndexPage subChild) {
		int start = binarySearch(0, nEntries, key, value);
		if (start < 0) {
			start = -(start+1);
		}
		for (int i = start; i <= nEntries; i++) {
			if (subPages[i] == indexPage) {
				markPageDirtyAndClone();
				
				//remove page from FSM.
				ind.file.reportFreePage(subPageIds[i]);
				subPageIds[i] = subChild.pageId();
				subPages[i] = subChild;
				if (i>0) {
					keys[i-1] = subChild.getMinKey();
					if (!ind.isUnique()) {
						values[i-1] = subChild.getMinKeyValue();
					}
				}
				subChild.setParent(this);
				return;
			}
		}
//		System.out.println("this:" + parent);
//		this.printLocal();
//		System.out.println("sub-page:");
//		indexPage.printLocal();
		throw DBLogger.newFatalInternal("Sub-page not found.");
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
		if (nEntries == -1) {
			return Long.MIN_VALUE;
		}
		long max = ((LLIndexPage)getPageByPos(nEntries)).getMax();
		return max;
	}

	@Override
	protected AbstractIndexPage newInstance() {
		return new LLIndexPage(this);
	}

	/**
	 * Special method to remove entries. When removing the entry, it checks whether other entries
	 * in the given range exist. If none exist, the value is returned as free page to FSM.  
	 * @param key
	 * @param min
	 * @param max
	 * @return The previous value
	 */
	public long deleteAndCheckRangeEmpty(long key, long min, long max) {
        LLIndexPage pageKey = locatePageForKeyUnique(key, false);
		int posKey = pageKey.binarySearchUnique(0, pageKey.nEntries, key);
		//We assume that the key exists. Otherwise we get an exception anyway in the remove-method.
		//-> no such calculation: posKey = -(posKey+1);
		//First we cover the most frequent cases, which are also fastest to check.
		long[] keys = pageKey.getKeys();
		if (posKey > 0) {
			if (keys[posKey-1] >= min) {
                return pageKey.remove(key);
			}
			if (posKey < pageKey.nEntries-1) {
	            if (keys[posKey+1] <= max) {
	                return pageKey.remove(key);
	            }
	            //we are in the middle of the page surrounded by values outside the range
	            ind.file.reportFreePage(BitTools.getPage(key));
                return pageKey.remove(key);
			}
		} else if (posKey == 0 && pageKey.nEntries > 1) {
			if (keys[posKey+1] <= max) {
				return pageKey.remove(key);
			}
		}

		//brute force:
        long pos = pageKey.remove(key);
        LLEntryIterator iter = ind.iterator(min, max);
        if (!iter.hasNextULL()) {
            ind.file.reportFreePage(BitTools.getPage(key));
        }
        iter.close();
        return pos;
        
//        //If we get here, the key is on the border of the page and we need to search more.
//		//If we get here, we also know that there are no values from the range on the page.
//		
//		LLIndexPage pageMin = locatePageForKeyUnique(min, false);
//        if (pageKey != pageMin) {
////            System.out.println("X6");
//        	return pageKey.remove(key);
//        }
//        LLIndexPage pageMax = locatePageForKeyUnique(max, false);
//        if (pageKey != pageMax) {
////            System.out.println("X7");
//        	return pageKey.remove(key);
//        }
//
//        //Now we know that there are no range-keys on other pages either. We can remove the page.
//        
//        System.out.println("X8");
//		fsm.reportFreePage(BitTools.getPage(key));
//    	return pageKey.remove(key);
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

	@Override
	final void setNEntries(int n) {
		nEntries = (short) n;
	}
}
