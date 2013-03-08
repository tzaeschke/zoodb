/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.profiler.test.server;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.StorageInMemory;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;


/**
 * Test harness for a rare problem when adding OIDs out of order to the OID index during commit.
 * 
 * The adding of 119 causes at some point a page-split which creates the new page such that it 
 * starts with the new value (119) only, regardless of any higher values on the old page.
 * The entry for key (128) still exists on the old page, but cannot be found anymore. 
 * 
 * This occurred with PageSize = 1024.
 * 
 * @author Tilmann Zäschke
 *
 */
public class TestOidIndex_003 {

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
    		130, 133143986344L, 
    		131, 133143986427L, 
    		132, 133143986510L, 
    		133, 133143986593L, 
    		134, 133143986678L, 
    		135, 133143986763L, 
    		136, 133143986848L, 
    		137, 133143986933L, 
    		138, 133143987016L, 
    		139, 133143987099L, 
    		140, 137438953472L, 
    		141, 137438953557L, 
    		142, 137438953642L, 
    		143, 137438953727L, 
    		144, 137438953810L, 
    		145, 137438953893L, 
    		146, 137438953976L, 
    		147, 137438954059L, 
    		148, 137438954142L, 
    		149, 137438954225L, 
    		150, 137438954310L, 
    		151, 137438954395L, 
    		152, 141733920768L, 
    		153, 141733920853L, 
    		154, 141733920936L, 
    		155, 141733921019L, 
    		156, 141733921104L, 
    		157, 141733921189L, 
    		158, 141733921274L, 
    		159, 141733921359L, 
    		160, 141733921442L, 
    		161, 141733921525L, 
    		162, 141733921608L, 
    		163, 141733921691L, 
    		164, 146028888064L, 
    		165, 146028888149L, 
    		166, 146028888234L, 
    		167, 146028888319L, 
    		168, 146028888402L, 
    		169, 146028888485L, 
    		170, 146028888570L, 
    		171, 146028888655L, 
    		172, 146028888740L, 
    		110, 146028888825L, 
    		111, 146028888897L, 
    		112, 146028888980L, 
    		113, 150323855360L, 
    		114, 150323855443L, 
    		115, 150323855526L, 
    		116, 150323855609L, 
    		117, 150323855692L, 
    		118, 150323855775L, 
    		119, 150323855858L, 
    		120, 150323855943L, 
    		121, 150323856028L, 
    		122, 150323856113L, 
    		123, 150323856198L, 
    		124, 150323856281L, 
    		125, 154618822656L, 
    		126, 154618822741L, 
    		127, 154618822826L
    };
    
    
    @Test
    public void testIndex() {
    	FreeSpaceManager fsm = new FreeSpaceManager();
    	StorageChannel paf = new StorageInMemory(1024, fsm);
    	//fsm.initBackingIndexLoad(paf, 7, 8);
    	fsm.initBackingIndexNew(paf);

    	PagedUniqueLongLong ind = new PagedUniqueLongLong(paf);
        
        //build index
        for (int i = 0; i < I.length; i+=2) {
        	long oid = I[i];
        	long val = I[i+1];
        	ind.insertLong(oid, val);
            assertNotNull( ind.findValue(oid) );
            if (i >= 20) {
            	assertNotNull("i=" + i, ind.findValue(128) );
            }
        }
    }
}
