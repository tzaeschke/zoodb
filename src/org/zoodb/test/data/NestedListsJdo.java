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

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.data.ListHolder.Procedure;
import org.zoodb.test.util.TestTools;

public class NestedListsJdo {

//	# nestedlists
//	#
//	# [objects]: number of objects to store as leafs
//	# [reuse]: number of objects to reuse from the total objects 
//	# [depth]: depth of the tree
//	nestedlists.objects=50,50,50
//	nestedlists.reuse=30,15,1
//	nestedlists.depth=4,4,4
	
	private final int objects = 50;
	private final int reuse = 30;
	private final int depth = 4;
	
	private PersistenceManager pm;

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		//TestTools.defineSchema(JB0.class, JB1.class, JB2.class, JB3.class, JB4.class,JdoTree.class);
		TestTools.defineSchema(ListHolder.class);
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
        create();
        close();
        
        open();
        read();
        close();
        
        open();
        update();//", false, true, false));
        close();
        open();
        delete();//", false, true, false));
        close();
        System.out.println("Checksum = " + mCheckSum);
	}

	
	
	public void create() {
		begin();
		store(ListHolder.generate(depth, objects, reuse));
		commit();
	}
	
	private static int n = 0;
	
	public void read() {
		begin();
		ListHolder root = root();
		root.accept(new Visitor<ListHolder>(){
			public void visit(ListHolder listHolder){
				addToCheckSum(listHolder);
				n++;
			}
		});
		System.out.println("nR=" + n);
		commit();
	}
	
	private ListHolder root() {
        Query query = db().newQuery(ListHolder.class, "this._name == '" + ListHolder.ROOT_NAME + "'");
        @SuppressWarnings("unchecked")
		Collection<ListHolder> result = (Collection<ListHolder>)query.execute();
        if(result.size() != 1){
        	throw new IllegalStateException();
        }
        Iterator<ListHolder> it = result.iterator();
        return it.next();
	}
	
	public void update() {
		begin();
		ListHolder root = root();
		addToCheckSum(root.update(depth, new Procedure<ListHolder>() {
			@Override
			public void apply(ListHolder obj) {
				store(obj);
			}
		}));
		commit();
	}

	public void delete() {
		begin();
		ListHolder root = root();
		n = 0;
		addToCheckSum(root.delete(depth, new Procedure<ListHolder>() {
			@Override
			public void apply(ListHolder listHolder) {
				delete(listHolder);
				n++;
			}
		}));
		System.out.println("nD=" + n);
		commit();
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
    
    private void store(Object obj){
        db().makePersistent(obj);
    }
    
	private void delete(Object obj) {
		db().deletePersistent(obj);
	}
    
    private void open(){
    	pm = TestTools.openPM();
    }
    
    private void close(){
        TestTools.closePM();
        pm = null;
    }

    private long mCheckSum;

    /**
     * Collecting a checksum to make sure every team does a complete job  
     */
    private void addToCheckSum(long l){
        mCheckSum += l;
    }
	private void addToCheckSum(CheckSummable checkSummable){
		addToCheckSum(checkSummable.checkSum());
	}
    
}
