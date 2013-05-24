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

package org.zoodb.test;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.data.CheckSummable;
import org.zoodb.test.data.JB0;
import org.zoodb.test.data.JB1;
import org.zoodb.test.data.JB2;
import org.zoodb.test.data.JB3;
import org.zoodb.test.data.JB4;
import org.zoodb.test.data.JdoTree;
import org.zoodb.test.data.JdoTreeVisitor;
import org.zoodb.test.testutil.TestTools;


/**
 * The used to cause a problem during delete. See TestOidIndex_003 for a more specific test for
 * the same problem.
 * 
 * @author Tilmann Zäschke
 */
public class Test_051_PolePosSepangJdo {
    
	private static final int DEPTH = 12; //8, 10, 12,14
	
	private PersistenceManager pm;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(JB0.class, JB1.class, JB2.class, JB3.class, JB4.class,JdoTree.class);
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
		close();
		
//		open();
//		read();
////		read_hot();
//		close();
//		
//		open();
//        delete();
//        close();
	}
	
	@Test
	public void testFull() {
		open();
		write();
		close();
		
		open();
		read();
		read_hot();
		close();
		
		open();
        delete();
        close();
	}
	
	private Object oid;
    
	public void write(){
		begin();
        JdoTree tree = JdoTree.createTree(DEPTH);
        db().makePersistent(tree);
        oid = db().getObjectId(tree);
		commit();
	}

	public void read(){
		begin();
        JdoTree tree = (JdoTree)db().getObjectById(oid, false);
        JdoTree.traverse(tree, new JdoTreeVisitor() {
            public void visit(JdoTree tree) {
                addToCheckSum(tree.getDepth());
            }
        });
		commit();
	}
    
    public void read_hot() {
        read();
    }

	public void delete(){
		begin();
        JdoTree tree = (JdoTree)db().getObjectById(oid, false);
        JdoTree.traverse(tree, new JdoTreeVisitor() {
            public void visit(JdoTree tree) {
                db().deletePersistent(tree);
            }
        });
		commit();
	}
    
	
	protected PersistenceManager db(){
		return pm;
	}
    
    public void begin(){
        db().currentTransaction().begin();
    }
    
    public void commit(){
        db().currentTransaction().commit();
    }
    
    public void open(){
    	pm = TestTools.openPM();
    }
    
    public void close(){
        TestTools.closePM();
        pm = null;
    }
    
    public void store(Object obj){
        db().makePersistent(obj);
    }
    
    protected void doQuery( Query q, Object param){
        Collection<?> result = (Collection<?>)q.execute(param);
        Iterator<?> it = result.iterator();
        while(it.hasNext()){
            Object o = it.next();
            if(o instanceof CheckSummable){
                addToCheckSum(((CheckSummable)o).checkSum());
            }
        }
    }
    
    protected void readExtent(Class<?> clazz){
        Extent<?> extent = db().getExtent( clazz, false );
        Iterator<?> itr = extent.iterator();
        while (itr.hasNext()){
            Object o = itr.next();
            if(o instanceof CheckSummable){
                addToCheckSum(((CheckSummable)o).checkSum());    
            }
        }
        extent.closeAll();
    }

    private long mCheckSum;
	
    
    /**
     * Collecting a checksum to make sure every team does a complete job  
     */
    public void addToCheckSum(long l){
        mCheckSum += l;
    }
    
    public long checkSum(){
        return mCheckSum; 
    }
    

}
