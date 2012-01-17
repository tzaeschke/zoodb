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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collection;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.util.TestProcessLauncher;
import org.zoodb.test.util.TestTools;

public class FlatObjectJdo extends JdoDriver {

//	# flatobject
//	#
//	# [objects]: number of objects to store
//	# [selects]: number of queries to be run against all objects
//	# [updates]: number of updates and deletes to be run
//	# [commitinterval]: when to perform an intermediate commit during write and delete
//	flatobject.objects=30000,100000,300000
//	flatobject.selects=3000,3000,3000
//	flatobject.updates=3000,3000,3000
//	flatobject.commitinterval=10000,10000,10000

	private int objects;
	private int selects;
	private int updates;
	private int commitInterval;
	
	@BeforeClass
	public static void beforeClass() {
		TestTools.createDb();
//		TestTools.defineSchema(JdoIndexedObject.class);
//		PersistenceManager pm = TestTools.openPM();
//		pm.currentTransaction().begin();
//		Schema.locate(pm, JdoIndexedObject.class).defineIndex("_int", false);
//		Schema.locate(pm, JdoIndexedObject.class).defineIndex("_string", false);
//		pm.currentTransaction().commit();
//		TestTools.closePM();

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
		
		ZooSchema.locateClass(pm, ComplexHolder2.class).defineIndex("i2", false);
		ZooSchema.locateClass(pm, InheritanceHierarchy2.class).defineIndex("i2", false);
		ZooSchema.locateClass(pm, JdoIndexedObject.class).defineIndex("_int", false);
		ZooSchema.locateClass(pm, JdoIndexedObject.class).defineIndex("_string", false);
		ZooSchema.locateClass(pm, ListHolder.class).defineIndex("_id", false);
		ZooSchema.locateClass(pm, ListHolder.class).defineIndex("_name", false);
		ZooSchema.locateClass(pm, JB2.class).defineIndex("b2", false);
		ZooSchema.locateClass(pm, JdoIndexedPilot.class).defineIndex("mName", false);
		ZooSchema.locateClass(pm, JdoIndexedPilot.class).defineIndex("mLicenseID", false);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	
	
	
	@Test
	public void test() {
		RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
		List<String> arguments = RuntimemxBean.getInputArguments();
		for (String a: arguments) {
			System.out.println("at=" + a);
		}
//		runE(30000, 3000, 3000, 10000);
//		runE(100000, 3000, 3000, 10000);
//		runE(300000, 3000, 3000, 10000);
		run(30000, 3000, 3000, 10000);
		run(100000, 3000, 3000, 10000);
		run(300000, 3000, 3000, 10000);
//		run(3000000, 3000, 3000, 10000);
	}
	
    private void runE(int objects, int selects, int updates, int commitInterval) {
        long t0;
        System.runFinalization();
        for (int i = 1; i <= 2; i++) {
            sleep(1000);
            System.gc();
            sleep(1000);
            t0 = System.currentTimeMillis();
            TestProcessLauncher.launchProcess(
                    //"-Xmx2g -Dfile.encoding=Cp1252", 
                    "-server -Dfile.encoding=Cp1252", 
                    FlatObjectJdo.class, 
                    new String[] {"" + objects, "" + selects, "" + updates, "" + commitInterval, "" + i});
            System.out.println("*** Time: " + (System.currentTimeMillis()-t0)/1000.);
        }
    }
    
    private void runE0(int objects, int selects, int updates, int commitInterval) {
        long t0;
        t0 = System.currentTimeMillis();
        TestProcessLauncher.launchProcess(
                //"-Xmx2g -Dfile.encoding=Cp1252", 
                "-Xprof -server -Dfile.encoding=Cp1252", 
                FlatObjectJdo.class, 
                new String[] {"" + objects, "" + selects, "" + updates, "" + commitInterval, "" + 0});
        System.out.println("*** Time: " + (System.currentTimeMillis()-t0)/1000.);
    }
    	
	public static void main(String[] args) {
		RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
		List<String> arguments = RuntimemxBean.getInputArguments();
		for (String a: arguments) {
			System.out.println("a=" + a);
		}
		
		System.out.println(System.getProperty("java.home"));
		System.out.println(System.getProperty("java.class.path"));
		int objects = Integer.parseInt(args[0]);
		int selects = Integer.parseInt(args[1]);
		int updates = Integer.parseInt(args[2]);
		int commitInterval = Integer.parseInt(args[3]);
		int action = Integer.parseInt(args[4]);
		System.out.println("params=" + objects + ", " + selects + ", " + updates + ", " 
				+ commitInterval);
		FlatObjectJdo foj = new FlatObjectJdo();
		foj.objects = objects;
		foj.selects = selects;
		foj.updates = updates;
		foj.commitInterval = commitInterval;
		foj.runIndividually(action);

		System.runFinalization();
		System.runFinalizersOnExit(true);
	}
	
	private void runIndividually(int action) {
	    if (action == 0) {
	        run(objects, selects, updates, commitInterval);
	        return;
	    }
	    
	    
		open();
		switch(action) {
		case 1: new JdoTeam().deleteAll(db()); break; 
		case 2: write(); break;
		case 3: queryIndexedString(); break; 
		case 4: queryIndexedInt();  break;
		case 5: update(); break;
		case 6: delete(); break;
		}
		close();
	}
	
	private void run(int objects, int selects, int updates, int commitInterval) {
		this.objects = objects;
		this.selects = selects;
		this.updates = updates;
		this.commitInterval = commitInterval;
		
		open();
		new JdoTeam().deleteAll(db());
		close();
		
		open();
		write();
		close();

		open();
		queryIndexedString();
		close();
	    
		open();
		queryIndexedInt();
		close();
	    
		open();
		update();
		close();

		open();
		delete();
		close();
	}
	
	
	
	public void write(){
		begin();
        initializeTestId(objects, commitInterval);
		while ( hasMoreTestIds()){
			JdoIndexedObject indexedObject = new JdoIndexedObject(nextTestId());
			store(indexedObject);
			if (doCommit()){
				commit();
				begin();
			}
            addToCheckSum(indexedObject);
		}
		commit();
	}
 
    public void queryIndexedString() {
        begin();
        initializeTestId(selects, commitInterval);
        String filter = "this._string == param";
        while(hasMoreTestIds()) {
            Query query = db().newQuery(JdoIndexedObject.class, filter);
            query.declareParameters("String param");
            doQuery(query, IndexedObject.queryString(nextTestId()));
        }
        commit();
    }
            
    public void queryIndexedInt() {
	begin();
        initializeTestId(selects, commitInterval);
        String filter = "this._int == param";
        while(hasMoreTestIds()) {
            Query query = db().newQuery(JdoIndexedObject.class, filter);
            query.declareParameters("Integer param");
            doQuery(query, nextTestId());
        }
        commit();
    }
	
    public void update() {
    	begin();
    	String filter = "this._int == param";
        initializeTestId(updates, commitInterval);
        while(hasMoreTestIds()) {
            Query query = db().newQuery(JdoIndexedObject.class, filter);
            query.declareParameters("Integer param");
            Collection result = (Collection)query.execute(nextTestId());
            JdoIndexedObject indexedObject = (JdoIndexedObject) result.iterator().next();
        	indexedObject.updateString();
            addToCheckSum(indexedObject);
        }
        commit();
	}
    
    public void delete() {
    	begin();
    	String filter = "this._int == param";
        initializeTestId(updates, commitInterval);
        while(hasMoreTestIds()) {
            Query query = db().newQuery(JdoIndexedObject.class, filter);
            query.declareParameters("Integer param");
            Collection result = (Collection)query.execute(nextTestId());
            JdoIndexedObject indexedObject = (JdoIndexedObject) result.iterator().next();
            addToCheckSum(indexedObject);
        	indexedObject.updateString();
        	delete(indexedObject);
        }
        commit();
    }

    long t1;
    private void open(){
    	t1 = System.currentTimeMillis();
    	prepare(TestTools.openPM());
    }
    
    private void close(){
    	closeDatabase();
    	System.out.println("t= " + (System.currentTimeMillis()-t1));
//        TestTools.closePM();
    }

    private void sleep(int millis) {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}
