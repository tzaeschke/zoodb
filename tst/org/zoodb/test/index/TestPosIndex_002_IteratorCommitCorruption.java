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
package org.zoodb.test.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashSet;

import org.junit.Test;
import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.StorageRootInMemory;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;
import org.zoodb.jdo.internal.server.index.PagedPosIndex.ObjectPosIterator;

/**
 * Check that the pos-index can be reset when a commit invalidates
 * the positions. 
 * 
 * TODO
 * The refresh() might only be a temporary solution for avoiding crashes
 * when going over transaction boundaries. It maybe removed in future.
 * 
 * @author Tilmann Zaeschke
 */
public class TestPosIndex_002_IteratorCommitCorruption {

	@Test
	public void testIteratorRefresh() {
		StorageChannel paf = new StorageRootInMemory(48);
		PagedPosIndex ind = new PagedPosIndex(paf);

		final int N = 100000;
		for (int i = 0; i < N; i++) {
		    ind.addPos(i, i%4096, (i+1)%10==0 ? i+1 : 0);
		}
		
		HashSet<Long> del = new HashSet<Long>();
		
        ObjectPosIterator it = ind.iteratorObjects();
		int i = 0;
		while (it.hasNextOPI()) {
		    long pos = it.nextPos();
            assertFalse(del.contains(pos));
            i++;
            if (i %19 == 0) {
                int start = BitTools.getPage(pos);
                for (int ii = start+10; ii < start+1000 && ii < N; ii+=23) {
                    long pos2 = BitTools.getPos(ii, ii%4096);
                    if (!del.contains(pos2)) {
                        ind.removePosLongAndCheck(pos2);
                        del.add(pos2);
                    }
                }
                //simulate commit
                for (int iii = 0; iii < (i%5)+1; iii++) {
                    ind.refreshIterators();
                }
            }
        }
		assertEquals(N, i+del.size());
	}

}
