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

public class InheritanceHierarchyJdo {
    
//	# inheritancehierarchy
//	#
//	# [objects]: number of objects to select from
//	# [selects]: number of queries run against all objects
//
//	inheritancehierarchy.objects=3000,10000,30000
//	inheritancehierarchy.selects=100,100,100


	private int objects = 3000;
	private int selects = 100;
	
	private PersistenceManager pm;

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(
				InheritanceHierarchy0.class, 
				InheritanceHierarchy1.class, 
				InheritanceHierarchy2.class, 
				InheritanceHierarchy3.class, 
				InheritanceHierarchy4.class);
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
//		inheritancehierarchy.objects=3000,10000,30000
//		inheritancehierarchy.selects=100,100,100

		run(3000, 100);
		run(10000, 100);
		run(30000, 100);
	}
	
	
	private void run(int objects, int selects) {
		this.objects = objects;
		this.selects = selects;
		
        open();
        write();
        close();
        
        open();
        read();
        close();
        
        open();
        query();//", false, true, false));
        close();
        
        open();
        delete();//", false, true, false));
        close();
        System.out.println("Checksum = " + mCheckSum);
	}

    public void write(){
        begin();
        int count = objects;
        for (int i = 1; i<= count; i++) {
            InheritanceHierarchy4 inheritanceHierarchy4 = new InheritanceHierarchy4();
            inheritanceHierarchy4.setAll(i);
            store(inheritanceHierarchy4);
        }
        commit();
    }
    
    public void read(){
	begin();
        readExtent(InheritanceHierarchy4.class);
        commit();
    }
    
	public void query(){
	begin();
        int count = selects;
        String filter = "this.i2 == param";
        for (int i = 1; i <= count; i++) {
            Query query = db().newQuery(InheritanceHierarchy4.class, filter);
            query.declareParameters("int param");
            doQuery(query, i);
        }
        commit();
    }
    
	public void delete(){
        begin();
        Extent extent = db().getExtent(InheritanceHierarchy4.class, false);
        Iterator it = extent.iterator();
        while(it.hasNext()){
            db().deletePersistent(it.next());
            addToCheckSum(5);
        }
        extent.closeAll();
        commit();
    }

    protected void doQuery( Query q, Object param){
        Collection result = (Collection)q.execute(param);
        Iterator it = result.iterator();
        while(it.hasNext()){
            Object o = it.next();
            if(o instanceof CheckSummable){
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
    
    protected void readExtent(Class clazz){
        Extent extent = db().getExtent( clazz, false );
        int count = 0;
        Iterator itr = extent.iterator();
        while (itr.hasNext()){
            Object o = itr.next();
            count++;
            if(o instanceof CheckSummable){
                addToCheckSum(((CheckSummable)o).checkSum());  
            }
        }
        extent.closeAll();
    }
    
    private PersistenceManager db(){
		return pm;
	}
    
	private void store(Object pc) {
        db().makePersistent(pc);
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
