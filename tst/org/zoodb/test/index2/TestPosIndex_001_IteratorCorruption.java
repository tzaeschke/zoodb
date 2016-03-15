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
package org.zoodb.test.index2;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
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
		StorageChannel paf = new StorageRootInMemory(48);
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
