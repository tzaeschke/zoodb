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

import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;


public class ImolaJdo {
    
	//	# imola
	//	#
	//	# [objects]: number of objects to select from
	//	# [selects]: number of queries run against all objects
	//	# [commitinterval]: when to perform an intermediate commit during write and delete
	//
	//	imola.objects=30000,100000,300000
	//	imola.selects=5000,5000,5000
	//	imola.commitinterval=1000,1000,1000
	
	private static final int objectCount[] = {30000, 100000, 300000};
	private static final int selectCount = 5000;
	private static final int commitInterval = 1000;
	private int pos = 0;
	
	private PersistenceManager pm;
    private Object[] oids;
    
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(JB0.class, JB1.class, JB2.class, JB3.class, JB4.class,JdoTree.class);
		TestTools.defineSchema(JdoPilot.class, JdoIndexedPilot.class);
	}

//	@Before
//	public void beforeTest() {
//		pm = TestTools.openPM();
//	}
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}

    
    @Test
    public void test() {
//    	new MelbourneJdo().test();
//    	
//    	new Test_051_PolePosSepangJdo().testFull();
//    	
//    	new BahrainJdo().test();
//
//    	for (int i = 0; i < 3; i++) {
//    		pos = i;
	       	open();
	    	store();
	    	retrieve();
	    	close();
//    	}

    }
    
    
    private void store() {
        int count = objectCount[pos];
        oids = new Object[selectCount];
        begin();
        for ( int i = 1; i <= count; i++ ){
            storePilot(i);
        }
        commit();
    }

    private void retrieve() {
    	beginRead();
        for(Object id: oids) {
            JdoPilot pilot=(JdoPilot)db().getObjectById(id, false);
            if(pilot==null) {
                System.err.println("Object not found by ID.");
            }else{
                addToCheckSum(pilot.getPoints());
            }
        }   
    }

    private void storePilot(int idx) {
        JdoPilot pilot = new JdoPilot( "Pilot_" + idx, "Jonny_" + idx, idx , idx );
        db().makePersistent( pilot );
        
        if (idx <= selectCount) {
            oids[idx - 1] =  db().getObjectId(pilot);
        }
        if ( isCommitPoint(idx) ){
            db().currentTransaction().commit();
            db().currentTransaction().begin();
        }
    }

    private boolean isCommitPoint(int idx) {
        return commitInterval> 0  &&  idx%commitInterval==0 && idx<objectCount[pos];
    }
    
//    @Override
//    public void copyStateFrom(DriverBase masterDriver) {
//    	ImolaJdo master = (ImolaJdo) masterDriver;
//    	oids = master.oids;
//    }

    private PersistenceManager db(){
		return pm;
	}
    
	private void begin(){
        db().currentTransaction().begin();
    }
    
    private void commit(){
        db().currentTransaction().commit();
    }
    
    private void open(){
    	pm = TestTools.openPM();
    }
    
    private void close(){
        TestTools.closePM();
        pm = null;
    }

    private void beginRead(){
		Transaction currentTransaction = db().currentTransaction();
		if(! currentTransaction.isActive()){
			currentTransaction.begin();
		}
	}
	
    //private long mCheckSum;

    /**
     * Collecting a checksum to make sure every team does a complete job  
     */
    private void addToCheckSum(long l){
        //mCheckSum += l;
    }
}
