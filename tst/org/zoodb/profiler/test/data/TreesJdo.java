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

package org.zoodb.profiler.test.data;

import javax.jdo.PersistenceManager;

import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.profiler.test.util.TestTools;
import org.zoodb.profiler.test.data.ComplexHolder0;
import org.zoodb.profiler.test.data.ComplexHolder1;
import org.zoodb.profiler.test.data.ComplexHolder2;
import org.zoodb.profiler.test.data.ComplexHolder3;
import org.zoodb.profiler.test.data.ComplexHolder4;
import org.zoodb.profiler.test.data.DriverBase;
import org.zoodb.profiler.test.data.InheritanceHierarchy0;
import org.zoodb.profiler.test.data.InheritanceHierarchy1;
import org.zoodb.profiler.test.data.InheritanceHierarchy2;
import org.zoodb.profiler.test.data.InheritanceHierarchy3;
import org.zoodb.profiler.test.data.InheritanceHierarchy4;
import org.zoodb.profiler.test.data.JB0;
import org.zoodb.profiler.test.data.JB1;
import org.zoodb.profiler.test.data.JB2;
import org.zoodb.profiler.test.data.JB3;
import org.zoodb.profiler.test.data.JB4;
import org.zoodb.profiler.test.data.JN1;
import org.zoodb.profiler.test.data.JdoDriver;
import org.zoodb.profiler.test.data.JdoIndexedObject;
import org.zoodb.profiler.test.data.JdoIndexedPilot;
import org.zoodb.profiler.test.data.JdoLightObject;
import org.zoodb.profiler.test.data.JdoListHolder;
import org.zoodb.profiler.test.data.JdoPilot;
import org.zoodb.profiler.test.data.JdoTeam;
import org.zoodb.profiler.test.data.JdoTree;
import org.zoodb.profiler.test.data.JdoTreeVisitor;
import org.zoodb.profiler.test.data.ListHolder;
import org.zoodb.profiler.test.data.TreesJdo;



public class TreesJdo extends JdoDriver {

    
    private Object oid;

    //    # trees
//    #
//    # [depth]: depth of the tree
//
//    trees.depth=10,12,14
    
    private int depth;
    
    @BeforeClass
    public static void beforeClass() {
        TestTools.createDb();
//      TestTools.defineSchema(JdoIndexedObject.class);
//      PersistenceManager pm = TestTools.openPM();
//      pm.currentTransaction().begin();
//      Schema.locate(pm, JdoIndexedObject.class).defineIndex("_int", false);
//      Schema.locate(pm, JdoIndexedObject.class).defineIndex("_string", false);
//      pm.currentTransaction().commit();
//      TestTools.closePM();

        TestTools.defineSchema(JB0.class, JB1.class, JB2.class, JB3.class, JB4.class);
        
        TestTools.defineSchema(ComplexHolder0.class, ComplexHolder1.class, 
                ComplexHolder2.class, ComplexHolder3.class, ComplexHolder4.class);
        TestTools.defineSchema(InheritanceHierarchy0.class, InheritanceHierarchy1.class,
                InheritanceHierarchy2.class, InheritanceHierarchy3.class, InheritanceHierarchy4.class);

        TestTools.defineSchema(JdoIndexedObject.class, JdoIndexedPilot.class, 
                JdoLightObject.class, JdoListHolder.class, JdoPilot.class, JdoTree.class,
                ListHolder.class, JN1.class);
        
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        ZooSchema.locateClass(pm, ComplexHolder2.class).locateField("i2").createIndex(false);
        ZooSchema.locateClass(pm, InheritanceHierarchy2.class).locateField("i2").createIndex(false);
        ZooSchema.locateClass(pm, JdoIndexedObject.class).locateField("_int").createIndex(false);
        ZooSchema.locateClass(pm, JdoIndexedObject.class).locateField("_string").createIndex(false);
        ZooSchema.locateClass(pm, ListHolder.class).locateField("_id").createIndex(false);
        ZooSchema.locateClass(pm, ListHolder.class).locateField("_name").createIndex(false);
        ZooSchema.locateClass(pm, JB2.class).locateField("b2").createIndex(false);
        ZooSchema.locateClass(pm, JdoIndexedPilot.class).locateField("mName").createIndex(false);
        ZooSchema.locateClass(pm, JdoIndexedPilot.class).locateField("mLicenseID").createIndex(false);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
    long t1;
    private void open() {
        t1 = System.currentTimeMillis();
        prepare(TestTools.openPM());
    }
    
    private void close(String pre) {
        closeDatabase();
        System.out.println(pre + "t= " + (System.currentTimeMillis()-t1));
    }

    
    
    
    @Test
    public void test() {
        run(10);
        run(12);
        run(14);
    }
    
    private void run(int depth) {
        this.depth = depth;
        
        open();
        new JdoTeam().deleteAll(db());
        close("del-all-");
        
        open();
        write();
        close("wrt-");

        open();
        read();
        close("read-");
        
        open();
        delete();
        close("del-");
    }
    
    
    

    
    
    
    
	public void write(){
		begin();
        JdoTree tree = JdoTree.createTree(depth);
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
	
    @Override
    public void copyStateFrom(DriverBase masterDriver) {
    	TreesJdo master = (TreesJdo) masterDriver;
    	oid = master.oid;
    }

}
