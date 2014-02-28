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

import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.testutil.TestTools;



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
        
        ZooSchema.locateClass(pm, ComplexHolder2.class).createIndex("i2", false);
        ZooSchema.locateClass(pm, InheritanceHierarchy2.class).createIndex("i2", false);
        ZooSchema.locateClass(pm, JdoIndexedObject.class).createIndex("_int", false);
        ZooSchema.locateClass(pm, JdoIndexedObject.class).createIndex("_string", false);
        ZooSchema.locateClass(pm, ListHolder.class).createIndex("_id", false);
        ZooSchema.locateClass(pm, ListHolder.class).createIndex("_name", false);
        ZooSchema.locateClass(pm, JB2.class).createIndex("b2", false);
        ZooSchema.locateClass(pm, JdoIndexedPilot.class).createIndex("mName", false);
        ZooSchema.locateClass(pm, JdoIndexedPilot.class).createIndex("mLicenseID", false);
        
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
