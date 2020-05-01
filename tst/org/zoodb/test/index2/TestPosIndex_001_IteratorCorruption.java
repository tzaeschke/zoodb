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
package org.zoodb.test.index2;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.server.index.PagedPosIndex;

/**
 * Check rare occurrences where the pos index iterator got corrupted
 * when deleting elements. 
 * 
 * @author Tilmann Zaeschke
 */
public class TestPosIndex_001_IteratorCorruption {

	@Test
	public void testIndexUnique() {
		IOResourceProvider paf = new StorageRootInMemory(48).createChannel();
		PagedPosIndex ind = new PagedPosIndex(paf);

		final int N = 1000000;
		for (int i = 0; i < N; i++) {
		    ind.addPos(i, i%4096, (i+1)%10==0 ? i+1 : 0);
		}
		
		
//		ObjectPosIterator it = ind.iteratorObjects();
//		int i = 0;
//		while (it.hasNextOPI()) {
//		    long pos = it.nextPos();
//            int offs = BitTools.getOffs(pos);
//            int page = BitTools.getPage(pos);
//            assertEquals(page, i);
//            assertEquals(offs, i%4096);
//            i++;
//            if (i %50 == 0) {
//                for (int ii = i-50; ii < i; ii++) {
//                    long pos2 = BitTools.getPos(ii, ii%4096);
//                    ind.removePosLongAndCheck(pos2);
//                }
//            }
//        }
//		assertEquals(N, i);
		for (int i = 1; i <= N; i++) {
            if (i %50 == 0) {
                for (int ii = i-50; ii < i; ii++) {
                    long pos2 = BitTools.getPos(ii, ii%4096);
                    ind.removePosLongAndCheck(pos2);
                }
            }
        }
		
		assertEquals(false, ind.iteratorObjects().hasNextOPI());
		
	}

}
