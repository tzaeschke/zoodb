package org.zoodb.test.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.PageAccessFileInMemory;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;


/**
 * Test harness for a rare problem when adding OIDs out of order to the OID index during commit.
 * 
 * The adding of 1 after a page with higher values was already full caused creation of a new
 * page which was inserted at the wrong position. 
 * 
 * @author Tilmann Zäschke
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
		FreeSpaceManager fsm = new FreeSpaceManager();
		PageAccessFile paf = new PageAccessFileInMemory(128, fsm);
		//fsm.initBackingIndexLoad(paf, 7, 8);
		fsm.initBackingIndexNew(paf);

		PagedUniqueLongLong ind = new PagedUniqueLongLong(paf);

		//build index
		for (int i = 0; i < I.length; i+=2) {
			long oid = I[i];
			long val = I[i+1];
			ind.insertLong(oid, val);
			LLEntry e = ind.findValue(oid);
//			if (e==null) {
//				ind.print();
//			}
			assertNotNull( "oid=" + oid, ind.findValue(oid) );
		}

		for (int i = 0; i < I.length; i+=2) {
			long oid = I[i];
			long val = I[i+1];
			LLEntry e = ind.findValue(oid);
//			if (e==null) {
//				ind.print();
//			}
			assertNotNull("i=" + i + "  oid=" + oid + "  val=" + val,  e);
			assertEquals(val, e.getValue());
		}
	}
}
