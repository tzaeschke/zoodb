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

package org.zoodb.test.data;

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOFatalInternalException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.util.TestTools;



public class BahrainJdo {
	
	//	#  bahrain
	//	#
	//	# [objects]: number of objects to select from
	//	# [selects]: number of queries to be run against all objects
	//	# [updates]: number of updates to be run
	//	# [commitinterval]: when to perform an intermediate commit during write and delete
	//
	//	bahrain.objects=3000,10000,30000
	//	bahrain.selects=900,900,900
	//	bahrain.updates=800,800,800
	//	bahrain.commitinterval=1000,1000,1000
	
	private static final int objectCount = 3000;
	private static final int selectCount = 900;
	private static final int updateCount = 800;
	private static final int commitInterval = 1000;
	
	private PersistenceManager pm;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		//TestTools.defineSchema(JB0.class, JB1.class, JB2.class, JB3.class, JB4.class,JdoTree.class);
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
        queryIndexedString();
        queryIndexedInt();
        close();
        open();
        update();//", false, true, false));
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
            JdoIndexedPilot p = new JdoIndexedPilot( "Pilot_" + i, "Jonny_" + i, i , i );
            db().makePersistent( p );
            addToCheckSum(i);
            if ( commitInterval > 0  &&  ++commitctr >= commitInterval ){
                commitctr = 0;
                commit();
                begin();
            }
        }
        
        commit();
    }
    
    
 
    private void queryIndexedString() {
        begin();
        
        String filter = "this.mName == param";
        for (int i = 1; i <= selectCount; i++) {
            Query query = db().newQuery(JdoIndexedPilot.class, filter);
            query.declareParameters("String param");
            doQuery(query, "Pilot_" + i);
        }
        
        commit();
    }
    
    
            
//    private void queryString() {
//        String filter = "this.mFirstName == param";
//        for (int i = 1; i <= selectCount; i++) {
//            Query query = db().newQuery(JdoIndexedPilot.class, filter);
//            query.declareParameters("String param");
//            doQuery(query, "Jonny_" + i);
//        }
//    }

    private void queryIndexedInt() {
        begin();
        
        String filter = "this.mLicenseID == param";
        for (int i = 1; i <= selectCount; i++) {
            Query query = db().newQuery(JdoIndexedPilot.class, filter);
            query.declareParameters("Integer param");
            doQuery(query, new Integer(i));
        }
        
        commit();
    }

//    private void queryInt() {
//        String filter = "this.mPoints == param";
//        for (int i = 1; i <= selectCount; i++) {
//            Query query = db().newQuery(JdoIndexedPilot.class, filter);
//            query.declareParameters("Integer param");
//            doQuery(query, new Integer(i));
//        }
//    }

    private void update() {
		PersistenceManager pm = db();
	    pm.currentTransaction().begin();
	    Extent<?> extent = pm.getExtent(JdoIndexedPilot.class, false);
	    Iterator<?> it = extent.iterator();
	    for (int i = 1; i <= updateCount; i++) {
	        JdoIndexedPilot p = (JdoIndexedPilot)it.next();
	        p.setName( p.getName().toUpperCase() );
	        addToCheckSum(1);
	    }
	    extent.closeAll();
	    pm.currentTransaction().commit();
	}
    
    private void delete() {
        begin();
        int commitctr = 0;
        Extent<?> extent = db().getExtent(JdoIndexedPilot.class, false);
        Iterator<?> it = extent.iterator();
        while(it.hasNext()){
            db().deletePersistent(it.next());
            addToCheckSum(1);
            if ( commitInterval > 0  &&  ++commitctr >= commitInterval ){
                commitctr = 0;
                commit();
                begin();
            }
        }
        extent.closeAll();
        commit();
    }

    private void doQuery( Query q, Object param){
    	beginRead();
        Collection<?> result = (Collection<?>)q.execute(param);
        Iterator<?> it = result.iterator();
        while (it.hasNext()){
            Object o = it.next();
            if (o instanceof CheckSummable){
            	try{
            		addToCheckSum(((CheckSummable)o).checkSum());
            	} catch(JDOFatalInternalException e){
            		Throwable[] nestedExceptions = e.getNestedExceptions();
            		if(nestedExceptions != null){
            			for (int i = 0; i < nestedExceptions.length; i++) {
            				nestedExceptions[i].printStackTrace();
						}
            		}
            		
            	}
            }
        }
    }
    
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
	
    private long mCheckSum;

    /**
     * Collecting a checksum to make sure every team does a complete job  
     */
    private void addToCheckSum(long l){
        mCheckSum += l;
    }
}
