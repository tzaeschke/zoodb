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


import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class MelbourneJdo {
	
	//	# melbourne
	//	#
	//	# [objects]: number of objects to be written, read and deleted
	//	# [commitintervall]: when to perform an intermediate commit during write and delete
	//
	//	melbourne.objects=10000,30000,100000
	//	melbourne.commitinterval=1000,1000,1000
	
	private static final int objectCount = 10000;
	private static final int commitInterval = 1000;
	
	private PersistenceManager pm;
    
	
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
    	open();
        write();
        //re-enable for proper Melbourne test
        read();
        close();
        open();
        delete();//", false, true, false));
        close();
    }

	public void write(){
        
		begin();
        
        int numobjects = objectCount;
		
		int commitctr = 0;
		for ( int i = 1; i <= numobjects; i++ ){
            
			JdoPilot p = new JdoPilot( "Pilot_" + i, i );
			db().makePersistent( p );
			
			if ( commitInterval > 0  &&  ++commitctr >= commitInterval ){
				commitctr = 0;
				commit();
				begin();
				System.out.println( "commit while writing at " + i+1 ); //NOI18N
			}
            addToCheckSum(i);
		}
		
		commit();
	}
	
	public void read(){
        readExtent(JdoPilot.class);
	}
    
    public void read_hot() {
        read();
    }
    
	public void delete(){
        
        int numobjects = objectCount;

		begin();
		
		Extent<?> extent = db().getExtent( JdoPilot.class, false );
		Iterator<?> itr = extent.iterator();
		int commitctr = 0;
		for ( int i = 0; i < numobjects; i++ ){
            
			JdoPilot p = (JdoPilot)itr.next();
			db().deletePersistent( p );
            
			if ( commitInterval > 0  && ++commitctr >= commitInterval ){
				commitctr = 0;
                
                // Debugging VOA: commit() seems to close the extent anyway
                // so we can do it here
                extent.closeAll();
				
                commit();
				begin();
				System.out.println( "commit while deleting at " + i+1 ); //NOI18N
                
                // Debugging VOA: If we close the extent, we have to open it
                // again of course.
                extent = db().getExtent( JdoPilot.class, false );
                itr = extent.iterator();
			}
		}
        
		commit();
		
		extent.closeAll();
	}


    private void readExtent(Class<?> clazz){
    	beginRead();
        Extent<?> extent = db().getExtent( clazz, false );
        int count = 0;
        Iterator<?> itr = extent.iterator();
        while (itr.hasNext()){
            Object o = itr.next();
            count++;
            if(o instanceof CheckSummable){
                addToCheckSum(((CheckSummable)o).checkSum());  
            }
        }
        extent.closeAll();
		
		//ensure that n is not optimized away
		if (count == 0) {
			throw new IllegalStateException();
		}
    }
    
//    private void doQuery( Query q, Object param){
//    	beginRead();
//        Collection<?> result = (Collection<?>)q.execute(param);
//        Iterator<?> it = result.iterator();
//        while(it.hasNext()){
//            Object o = it.next();
//            if(o instanceof CheckSummable){
//            	try{
//            		addToCheckSum(((CheckSummable)o).checkSum());
//            	} catch(JDOFatalInternalException e){
//            		Throwable[] nestedExceptions = e.getNestedExceptions();
//            		if(nestedExceptions != null){
//            			for (int i = 0; i < nestedExceptions.length; i++) {
//            				nestedExceptions[i].printStackTrace();
//						}
//            		}
//            		
//            	}
//            }
//        }
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
	
//    private long mCheckSum;

    /**
     * Collecting a checksum to make sure every team does a complete job  
     */
    private void addToCheckSum(long l){
        //mCheckSum += l;
    }
    
//    private long checkSum(){
//        return mCheckSum; 
//    }
}
