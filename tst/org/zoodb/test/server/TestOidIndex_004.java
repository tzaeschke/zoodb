/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.zoodb.jdo.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.StorageRootInMemory;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;


/**
 * Test harness for a rare problem when adding OIDs out of order to the OID index during commit.
 * 
 * The adding of 1 after a page with higher values was already full caused creation of a new
 * page which was inserted at the wrong position. 
 * 
 * @author Tilmann Zaeschke
 */
public class TestOidIndex_004 {

	private final long[] I = {
			102, 30064771072L,
			103, 34359738368L,
			101, 38654705664L,
			104, 68719476736L,
			108, 73014444032L,
			105, 77309411328L,
			107, 81604378624L,
			109, 85899345920L,
			106, 90194313216L,
			128, 133143986176L, 
			129, 133143986261L, 
			1, 1,
			2, 2,
			3, 3,
			4, 4,
			5, 5,
	};


	@Test
	public void testIndex() {
		StorageChannel paf = new StorageRootInMemory(128);

		PagedUniqueLongLong ind = new PagedUniqueLongLong(DATA_TYPE.GENERIC_INDEX, paf);

		//build index
		for (int i = 0; i < I.length; i+=2) {
			long oid = I[i];
			long val = I[i+1];
			ind.insertLong(oid, val);
			LLEntry e = ind.findValue(oid);
			assertNotNull( "oid=" + oid, e );
		}

		for (int i = 0; i < I.length; i+=2) {
			long oid = I[i];
			long val = I[i+1];
			LLEntry e = ind.findValue(oid);
			assertNotNull("i=" + i + "  oid=" + oid + "  val=" + val,  e);
			assertEquals(val, e.getValue());
		}
	}
}
