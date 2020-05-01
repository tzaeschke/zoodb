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
package org.zoodb.test.index;


/**
 * Check that the pos-index can be reset when a commit invalidates
 * the positions. 
 * 
 * @author Tilmann Zaeschke
 */
public class TestPosIndex_002_IteratorCommitCorruption {

// This has been disallowed, now more COW!
//
//	@Test
//	public void testIteratorRefresh() {
//		StorageChannel paf = new StorageRootInMemory(48);
//		PagedPosIndex ind = new PagedPosIndex(paf);
//
//		final int N = 100000;
//		for (int i = 0; i < N; i++) {
//		    ind.addPos(i, i%4096, (i+1)%10==0 ? i+1 : 0);
//		}
//		
//		HashSet<Long> del = new HashSet<Long>();
//		
//        ObjectPosIterator it = ind.iteratorObjects();
//		int i = 0;
//		while (it.hasNextOPI()) {
//		    long pos = it.nextPos();
//            assertFalse(del.contains(pos));
//            i++;
//            if (i %19 == 0) {
//                int start = BitTools.getPage(pos);
//                for (int ii = start+10; ii < start+1000 && ii < N; ii+=23) {
//                    long pos2 = BitTools.getPos(ii, ii%4096);
//                    if (!del.contains(pos2)) {
//                        ind.removePosLongAndCheck(pos2);
//                        del.add(pos2);
//                    }
//                }
//                //simulate commit
//                for (int iii = 0; iii < (i%5)+1; iii++) {
//                    ind.refreshIterators();
//                }
//            }
//        }
//		assertEquals(N, i+del.size());
//	}

}
