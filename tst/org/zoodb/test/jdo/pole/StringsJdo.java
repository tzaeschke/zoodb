/* 
This file is part of the PolePosition database benchmark
http://www.polepos.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public
License along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
MA  02111-1307, USA. */

package org.zoodb.test.jdo.pole;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.test.testutil.TestTools;

/**
 * @author Christian Ernst
 */
public class StringsJdo extends JdoDriver {
    
	public static final Logger LOGGER = LoggerFactory.getLogger(StringsJdo.class);


//    # strings
//    #
//    # [objects]: number of objects to be written, read and deleted
//    # [commitintervall]: when to perform an intermediate commit during write and delete
//
//    strings.objects=10000,30000,100000
//    strings.commitinterval=1000,1000,1000

    private int objects;
    private int commitInterval;

    @BeforeClass
    public static void beforeClass() {
        TestTools.createDb();
        TestTools.defineSchema(JN1.class);
    }

    @Test
    public void test() {
//        open();
//        close("nix");
//        open();
//        int N = 100000 + 30000 + 10000;
//        JN1[] x = new JN1[N];
//        for ( int i = 1; i <= N; i++ ){
//            x[i-1] = JN1.generate(i);
//        }
//        close("trans-");
//        System.out.println(x[0].s0);
        
        
        run(10000, 1000);
        run(30000, 1000);
        run(100000, 1000);
    }
    
    private void run(int objects, int commitInterval) {
        this.objects = objects;
        this.commitInterval = commitInterval;
        open();
        write();
        close("wrt-");
        
        open();
        read();
        close("read-");
    }

    long t1;
    private void open() {
        t1 = System.currentTimeMillis();
        prepare(TestTools.openPM());
    }
    
    private void close(String pre) {
//        System.out.println("Mem-tot: " + Runtime.getRuntime().totalMemory());
//        System.out.println("Mem-max: " + Runtime.getRuntime().maxMemory());
//        System.out.println("Mem-fre: " + Runtime.getRuntime().freeMemory());
        long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        LOGGER.info("Mem-: " + mem);
        LOGGER.info("Mem-x: " + mem/140000);
//        System.gc(); System.gc(); System.gc(); System.gc();
//        System.gc(); System.gc(); System.gc(); System.gc();
//        System.gc(); System.gc(); System.gc(); System.gc();
//        System.gc(); System.gc(); System.gc(); System.gc();
//        long mem2= Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        System.out.println("Mem2-: " + mem2);
        closeDatabase();
        LOGGER.info(pre + "t= " + (System.currentTimeMillis()-t1));
//        TestTools.closePM();
        
        //Mem usage should be around 140.000 * JN1 = 140.000 + ~60(100?)bytes
        //Each JN1 has 10 references to the SAME String (40(base)+20*2(str)=60 bytes)!
        //TODO? Implement SCO de-duplication?
        //Assert.assertTrue("mem usage: " + mem, mem < 50*1000*1000);
        LOGGER.info("SCO de-duplication not implemented");
    }

    
    public void write(){
        
        int numobjects = this.objects;//setup().getObjectCount();
        int commitinterval  = this.commitInterval;//setup().getCommitInterval();
        int commitctr = 0;
        
        begin();
        for ( int i = 1; i <= numobjects; i++ ){
            store(JN1.generate(i));
            
            if ( commitinterval > 0  &&  ++commitctr >= commitinterval ){
                commitctr = 0;
                commit();
                begin();
            }
            
            addToCheckSum(i);
        }
        commit();
    }

    public void read() {
    	db().currentTransaction().begin();
    	readExtent(JN1.class);
    	db().currentTransaction().rollback();
    }

}
