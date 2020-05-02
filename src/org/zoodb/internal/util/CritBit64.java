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
package org.zoodb.internal.util;

/**
 * CritBit64 is a 1D crit-bit tree with 64bit key length.
 * 
 * In order to store floating point values, please convert them to 'long' with
 * BitTools.toSortableLong(...), also when supplying query parameters.
 * Extracted values can be converted back with BitTools.toDouble() or toFloat().
 * This conversion is taken from: 
 * T.Zaeschke, C.Zimmerli, M.C.Norrie:  The PH-Tree - A Space-Efficient Storage Structure and 
 * Multi-Dimensional Index (SIGMOD 2014)
 * 
 * Version 1.0
 * 
 * @author Tilmann Zaeschke
 */
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CritBit64<V> {

	private static final int DEPTH = 64;
	
	private Node<V> root;
	private long rootKey;
	private V rootVal;

	private int size;
	
	private static class Node<V> {
		V loVal;
		V hiVal;
		Node<V> lo;
		Node<V> hi;
		long loPost;
		long hiPost;
		long infix;
		int posFirstBit;  
		int posDiff;
		
		Node(int posFirstBit, long loPost, V loVal, long hiPost, V hiVal, 
				long infix, int posDiff) {
			this.loPost = loPost;
			this.loVal = loVal;
			this.hiPost = hiPost;
			this.hiVal = hiVal;
			this.infix = infix;
			this.posFirstBit = posFirstBit;
			this.posDiff = posDiff;
		}
	}
	
	private CritBit64() {
		//private 
	}
	
	/**
	 * Create a 1D crit-bit tree with 64 bit key length. 
	 * @return a 1D crit-bit tree
	 * @param <V> The type of the values
	 */
	public static <V> CritBit64<V> create() {
		return new CritBit64<V>();
	}
	
	/**
	 * Add a key value pair to the tree or replace the value if the key already exists.
	 * @param key The key
	 * @param val The value
	 * @return The previous value or {@code null} if there was no previous value
	 */
	public V put(long key, V val) {
		if (size == 0) {
			rootKey = key;
			rootVal = val;
			size++;
			return null;
		}
		if (size == 1) {
			Node<V> n2 = createNode(key, val, rootKey, rootVal, 0);
			if (n2 == null) {
				V prev = rootVal;
				rootVal = val;
				return prev; 
			}
			root = n2;
			rootKey = 0;
			rootVal = null;
			size++;
			return null;
		}
		Node<V> n = root;
		while (true) {
			if (n.posFirstBit != n.posDiff) {
				//split infix?
				int posDiff = compare(key, n.infix);
				if (posDiff < n.posDiff && posDiff != -1) {
					long subInfix = extractInfix(n.infix, n.posDiff-1);
					//new sub-node
					Node<V> newSub = new Node<V>(posDiff+1, n.loPost, n.loVal, n.hiPost, n.hiVal, 
							subInfix, n.posDiff);
					newSub.hi = n.hi;
					newSub.lo = n.lo;
					if (getBit(key, posDiff)) {
						n.hi = null;
						n.hiPost = key;
						n.hiVal = val;
						n.lo = newSub;
						n.loPost = 0;
						n.loVal = null;
					} else {
						n.hi = newSub;
						n.hiPost = 0;
						n.hiVal = null;
						n.lo = null;
						n.loPost = key;
						n.loVal = val;
					}
					n.infix = extractInfix(key, posDiff-1);
					n.posDiff = posDiff;
					size++;
					return null;
				}
			}			
			
			//infix matches, so now we check sub-nodes and postfixes
			if (getBit(key, n.posDiff)) {
				if (n.hi != null) {
					n = n.hi;
				} else {
					Node<V> n2 = createNode(key, val, n.hiPost, n.hiVal, n.posDiff + 1);
					if (n2 == null) {
						V prev = n.hiVal;
						n.hiVal = val;
						return prev; 
					}
					n.hi = n2;
					n.hiPost = 0;
					n.hiVal = null;
					size++;
					return null;
				}
			} else {
				if (n.lo != null) {
					n = n.lo;
				} else {
					Node<V> n2 = createNode(key, val, n.loPost, n.loVal, n.posDiff + 1);
					if (n2 == null) {
						V prev = n.loVal;
						n.loVal = val;
						return prev; 
					}
					n.lo = n2;
					n.loPost = 0;
					n.loVal = null;
					size++;
					return null;
				}
			}
		}
	}
	
	public void printTree() {
		System.out.println("Tree: \n" + toString());
	}
	
	@Override
	public String toString() {
		if (root == null) {
			if (rootVal != null) {
				return "-" + toBinary(rootKey) + " v=" + rootVal;
			}
			return "- -";
		}
		Node<V> n = root;
		StringBuilder s = new StringBuilder();
		printNode(n, s, "", 0);
		return s.toString();
	}
	
	private void printNode(Node<V> n, StringBuilder s, String level, int currentDepth) {
		char NL = '\n'; 
		if (n.posFirstBit != n.posDiff) {
			s.append(level).append("n: ").append(currentDepth).append("/").append(n.posDiff).append(" ")
					.append(toBinary(n.infix)).append(NL);
		} else {
			s.append(level).append("n: ").append(currentDepth).append("/").append(n.posDiff).append(" i=0").append(NL);
		}
		if (n.lo != null) {
			printNode(n.lo, s, level + "-", n.posDiff+1);
		} else {
			s.append(level).append(" ").append(toBinary(n.loPost)).append(" v=").append(n.loVal).append(NL);
		}
		if (n.hi != null) {
			printNode(n.hi, s, level + "-", n.posDiff+1);
		} else {
			s.append(level).append(" ").append(toBinary(n.hiPost)).append(" v=").append(n.hiVal).append(NL);
		}
	}
	
	public boolean checkTree() {
		if (root == null) {
			return true;
		}
		if (rootVal == null) {
			System.err.println("root node AND value != null");
			return false;
		}
		return checkNode(root, 0);
	}
	
	private boolean checkNode(Node<V> n, int firstBitOfNode) {
		//check infix
		if (n.posDiff < firstBitOfNode) {
			System.err.println("infix with len=0 detected!");
			return false;
		}
		if (n.lo != null) {
			if (n.loPost != 0) {
				System.err.println("lo: sub-node AND value != 0");
				return false;
			}
			checkNode(n.lo, n.posDiff+1);
		}
		if (n.hi != null) {
			if (n.hiPost != 0) {
				System.err.println("hi: sub-node AND value != 0");
				return false;
			}
			checkNode(n.hi, n.posDiff+1);
		}
		return true;
	}
	
	private Node<V> createNode(long k1, V val1, long k2, V val2, int posFirstBit) {
		int posDiff = compare(k1, k2);
		if (posDiff == -1) {
			return null;
		}
		long infix = extractInfix(k1, posDiff-1);
		if (getBit(k2, posDiff)) {
			return new Node<V>(posFirstBit, k1, val1, k2, val2, infix, posDiff);
		} else {
			return new Node<V>(posFirstBit, k2, val2, k1, val1, infix, posDiff);
		}
	}
	
	/**
	 * 
	 * @param v value
	 * @param endPos last bit of infix, counting starts with 0 for 1st bit
	 * @return The infix PLUS leading bits before the infix that belong in the same 'long'.
	 */
	private static long extractInfix(long v, int endPos) {
		long inf = v;
		//avoid shifting by 64 bit which means 0 shifting in Java!
		if (endPos < 63) {
			inf &= ~((-1L) >>> (1+endPos)); // & 0x3f == %64
		}
		return inf;
	}

	/**
	 * @param n Node
	 * @param v Value
	 * @return True if the infix matches the value or if no infix is defined
	 */
	private boolean doesInfixMatch(Node<V> n, long v) {
		int endPos = n.posDiff-1;
		if (endPos >= 0 && v != n.infix) {
			long mask = 0x8000000000000000L >>> endPos;
			if ((v & mask) != (n.infix & mask)) {
				return false;
			}
			return true;
		}
		return true;
	}
	
	/**
	 * Compares two values.
	 * @param v1
	 * @param v2
	 * @return Position of the differing bit, or -1 if both values are equal
	 */
	private static int compare(long v1, long v2) {
		int pos = 0;
		if (v1 != v2) {
			long x = v1 ^ v2;
			pos += Long.numberOfLeadingZeros(x);
			return pos;
		}
		return -1;
	}

	/**
	 * Get the size of the tree.
	 * @return the number of keys in the tree
	 */
	public int size() {
		return size;
	}

	/**
	 * Check whether a given key exists in the tree.
	 * @param key The key
	 * @return {@code true} if the key exists otherwise {@code false}
	 */
	public boolean contains(long key) {
		if (size == 0) {
			return false;
		} 
		if (size == 1) {
			int posDiff = compare(key, rootKey);
			if (posDiff == -1) {
				return true;
			}
			return false;
		}
		Node<V> n = root;
		while (true) {
			if (!doesInfixMatch(n, key)) {
				return false;
			}			
			
			//infix matches, so now we check sub-nodes and postfixes
			if (getBit(key, n.posDiff)) {
				if (n.hi != null) {
					n = n.hi;
					continue;
				} 
				return compare(key,  n.hiPost) == -1;
			} else {
				if (n.lo != null) {
					n = n.lo;
					continue;
				}
				return compare(key,  n.loPost) == -1;
			}
			
		}
	}
	
	/**
	 * Get the value for a given key. 
	 * @param key The key
	 * @return the values associated with {@code key} or {@code null} if the key does not exist.
	 */
	public V get(long key) {
		if (size == 0) {
			return null;
		}
		if (size == 1) {
			int posDiff = compare(key, rootKey);
			if (posDiff == -1) {
				return rootVal;
			}
			return null;
		}
		Node<V> n = root;
		while (true) {
			if (!doesInfixMatch(n, key)) {
				return null;
			}			
			
			//infix matches, so now we check sub-nodes and postfixes
			if (getBit(key, n.posDiff)) {
				if (n.hi != null) {
					n = n.hi;
					continue;
				} 
				if (compare(key, n.hiPost) == -1) {
					return n.hiVal;
				}
			} else {
				if (n.lo != null) {
					n = n.lo;
					continue;
				}
				if (compare(key, n.loPost) == -1) {
					return n.loVal;
				}
			}
			return null;
		}
	}
	
	/**
	 * Remove a key and its value
	 * @param key The key
	 * @return The value of the key of {@code null} if the value was not found. 
	 */
	public V remove(long key) {
		if (size == 0) {
			return null;
		}
		if (size == 1) {
			int posDiff = compare(key, rootKey);
			if (posDiff == -1) {
				size--;
				rootKey = 0;
				V prev = rootVal;
				rootVal = null;
				return prev;
			}
			return null;
		}
		Node<V> n = root;
		Node<V> parent = null;
		boolean isParentHigh = false;
		while (true) {
			if (!doesInfixMatch(n, key)) {
				return null;
			}
			
			//infix matches, so now we check sub-nodes and postfixes
			if (getBit(key, n.posDiff)) {
				if (n.hi != null) {
					isParentHigh = true;
					parent = n;
					n = n.hi;
				} else {
					int posDiff = compare(key, n.hiPost);
					if (posDiff != -1) {
						return null;
					}
					//match! --> delete node
					//a) first recover other values
					long newPost = 0;
					if (n.lo == null) {
						newPost = n.loPost;
					}
					//b) replace data in parent node
					updateParentAfterRemove(parent, newPost, n.loVal, n.lo, isParentHigh, n);
					return n.hiVal;
				}
			} else {
				if (n.lo != null) {
					isParentHigh = false;
					parent = n;
					n = n.lo;
				} else {
					int posDiff = compare(key, n.loPost);
					if (posDiff != -1) {
						return null;
					}
					//match! --> delete node
					//a) first recover other values
					long newPost = 0;
					if (n.hi == null) {
						newPost = n.hiPost;
					}
					//b) replace data in parent node
					//for new infixes...
					updateParentAfterRemove(parent, newPost, n.hiVal, n.hi, isParentHigh, n);
					return n.loVal;
				}
			}
		}
	}
	
	private void updateParentAfterRemove(Node<V> parent, long newPost, V newVal,
			Node<V> newSub, boolean isParentHigh, Node<V> n) {
		
		if (parent == null) {
			rootKey = newPost;
			rootVal = newVal;
			root = newSub;
		} else if (isParentHigh) {
			if (newSub == null) {
				parent.hiPost = newPost;
				parent.hiVal = newVal;
			} else {
				parent.hiPost = 0;
				parent.hiVal = null;
			}
			parent.hi = newSub;
		} else {
			if (newSub == null) {
				parent.loPost = newPost;
				parent.loVal = newVal;
			} else {
				parent.loPost = 0;
				parent.loVal = null;
			}
			parent.lo = newSub;
		}
		if (newSub != null) {
			newSub.posFirstBit = n.posFirstBit;
			newSub.infix = extractInfix(newSub.infix, newSub.posDiff-1);
		}
		size--;
	}

	public CBIterator<V> iterator() {
		return new CBIterator<V>(this, DEPTH);
	}
	
	public static class CBIterator<V> implements Iterator<V> {
		private long nextKey = 0; 
		private V nextValue = null;
		private final Node<V>[] stack;
		 //0==read_lower; 1==read_upper; 2==go_to_parent
		private static final byte READ_LOWER = 0;
		private static final byte READ_UPPER = 1;
		private static final byte RETURN_TO_PARENT = 2;
		private final byte[] readHigherNext;
		private int stackTop = -1;

		@SuppressWarnings("unchecked")
		public CBIterator(CritBit64<V> cb, int DEPTH) {
			this.stack = new Node[DEPTH];
			this.readHigherNext = new byte[DEPTH];  // default = false

			if (cb.size == 0) {
				//Tree is empty
				return;
			}
			if (cb.size == 1) {
				nextValue = cb.rootVal;
				nextKey = cb.rootKey;
				return;
			}
			stack[++stackTop] = cb.root;
			findNext();
		}

		private void findNext() {
			while (stackTop >= 0) {
				Node<V> n = stack[stackTop];
				//check lower
				if (readHigherNext[stackTop] == READ_LOWER) {
					readHigherNext[stackTop] = READ_UPPER;
					if (n.lo == null) {
						nextValue = n.loVal;
						nextKey = n.loPost;
						return;
					} else {
						stack[++stackTop] = n.lo;
						readHigherNext[stackTop] = READ_LOWER;
						continue;
					}
				}
				//check upper
				if (readHigherNext[stackTop] == READ_UPPER) {
					readHigherNext[stackTop] = RETURN_TO_PARENT;
					if (n.hi == null) {
						nextValue = n.hiVal;
						nextKey = n.hiPost;
						--stackTop;
						return;
						//proceed to move up a level
					} else {
						stack[++stackTop] = n.hi;
						readHigherNext[stackTop] = READ_LOWER;
						continue;
					}
				}
				//proceed to move up a level
				--stackTop;
			}
			//Finished
			nextValue = null;
			nextKey = 0;
		}

		@Override
		public boolean hasNext() {
			return nextValue != null;
		}

		@Override
		public V next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			V ret = nextValue;
			findNext();
			return ret;
		}

		public long nextKey() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			long ret = nextKey;
			findNext();
			return ret;
		}

		public Entry<V> nextEntry() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			Entry<V> ret = new Entry<V>(nextKey, nextValue);
			findNext();
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}
	
	public QueryIterator<V> query(long min, long max) {
		return new QueryIterator<V>(this, min, max, DEPTH);
	}
	
	public static class QueryIterator<V> implements Iterator<V> {
		private final long minOrig;
		private final long maxOrig;
		private long nextKey = 0; 
		private V nextValue = null;
		private final Node<V>[] stack;
		 //0==read_lower; 1==read_upper; 2==go_to_parent
		private static final byte READ_LOWER = 0;
		private static final byte READ_UPPER = 1;
		private static final byte RETURN_TO_PARENT = 2;
		private final byte[] readHigherNext;
		private int stackTop = -1;

		@SuppressWarnings("unchecked")
		public QueryIterator(CritBit64<V> cb, long minOrig, long maxOrig, int DEPTH) {
			this.stack = new Node[DEPTH];
			this.readHigherNext = new byte[DEPTH];  // default = false
			this.minOrig = minOrig;
			this.maxOrig = maxOrig;

			if (cb.size == 0) {
				//Tree is empty
				return;
			}
			if (cb.size == 1) {
				checkMatchFullIntoNextVal(cb.rootKey, cb.rootVal);
				return;
			}
			Node<V> n = cb.root;
			if (!checkMatch(n.infix, n.posDiff)) {
				return;
			}
			stack[++stackTop] = cb.root;
			findNext();
		}

		private void findNext() {
			while (stackTop >= 0) {
				Node<V> n = stack[stackTop];
				//check lower
				if (readHigherNext[stackTop] == READ_LOWER) {
					readHigherNext[stackTop] = READ_UPPER;
					//TODO use bit directly to check validity
					long valTemp = setBit(n.infix, n.posDiff, false);
					if (checkMatch(valTemp, n.posDiff)) {
						if (n.lo == null) {
							if (checkMatchFullIntoNextVal(n.loPost, n.loVal)) {
								return;
							} 
							//proceed to check upper
						} else {
							stack[++stackTop] = n.lo;
							readHigherNext[stackTop] = READ_LOWER;
							continue;
						}
					}
				}
				//check upper
				if (readHigherNext[stackTop] == READ_UPPER) {
					readHigherNext[stackTop] = RETURN_TO_PARENT;
					long valTemp = setBit(n.infix, n.posDiff, true);
					if (checkMatch(valTemp, n.posDiff)) {
						if (n.hi == null) {
							if (checkMatchFullIntoNextVal(n.hiPost, n.hiVal)) {
								--stackTop;
								return;
							} 
							//proceed to move up a level
						} else {
							stack[++stackTop] = n.hi;
							readHigherNext[stackTop] = READ_LOWER;
							continue;
						}
					}
				}
				//proceed to move up a level
				--stackTop;
			}
			//Finished
			nextValue = null;
			nextKey = 0;
		}


		/**
		 * Full comparison on the parameter. Assigns the parameter to 'nextVal' if comparison
		 * fits.
		 * @param keyTemplate
		 * @return Whether we have a match or not
		 */
		private boolean checkMatchFullIntoNextVal(long keyTemplate, V value) {
			if ((minOrig > keyTemplate) || (keyTemplate > maxOrig)) { 
				return false;
			}
			nextValue = value;
			nextKey = keyTemplate;
			return true;
		}
		
		private boolean checkMatch(long keyTemplate, int currentDepth) {
			int toIgnore = 64 - currentDepth;
			long mask = (-1L) << toIgnore;
			if ((minOrig & mask) > (keyTemplate & mask)) {  
				return false;
			}
			if ((keyTemplate & mask) > (maxOrig & mask)) {  
				return false;
			}
			
			return true;
		}

		@Override
		public boolean hasNext() {
			return nextValue != null;
		}

		@Override
		public V next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			V ret = nextValue;
			findNext();
			return ret;
		}

		public long nextKey() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			long ret = nextKey;
			findNext();
			return ret;
		}

		public Entry<V> nextEntry() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			Entry<V> ret = new Entry<V>(nextKey, nextValue);
			findNext();
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}
	
	public static class Entry<V> {
		private final long key;
		private final V value;
		Entry(long key, V value) {
			this.key = key;
			this.value = value;		
		}
		public long key() {
			return key;
		}
		public V value() {
			return value;
		}
	}
	
    public static long setBit(long ba, int posBit, boolean b) {
        if (b) {
            return ba | (0x8000000000000000L >>> posBit);
        } else {
            return ba & (~(0x8000000000000000L >>> posBit));
        }
	}

	/**
	 * @param l The long value to get the bit from
	 * @param posBit Counts from left to right!!!
	 * @return The bit.
	 */
    public static boolean getBit(long l, int posBit) {
        //last 6 bit [0..63]
        return (l & (0x8000000000000000L >>> posBit)) != 0;
	}

	public static String toBinary(long l) {
		final int DEPTH = 64;
        StringBuilder sb = new StringBuilder();
        //long mask = DEPTH < 64 ? (1<<(DEPTH-1)) : 0x8000000000000000L;
        for (int i = 0; i < DEPTH; i++) {
            long mask = (1L << (long)(DEPTH-i-1));
            if ((l & mask) != 0) { sb.append("1"); } else { sb.append("0"); }
            if ((i+1)%8==0 && (i+1)<DEPTH) sb.append('.');
        	mask >>>= 1;
        }
        return sb.toString();
    }

}
