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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.profiler.test.util.TestTools;
import org.zoodb.profiler.test.data.CheckSummable;
import org.zoodb.profiler.test.data.ComplexHolder0;
import org.zoodb.profiler.test.data.ComplexHolder1;
import org.zoodb.profiler.test.data.ComplexHolder2;
import org.zoodb.profiler.test.data.ComplexHolder3;
import org.zoodb.profiler.test.data.ComplexHolder4;
import org.zoodb.profiler.test.data.NullVisitor;
import org.zoodb.profiler.test.data.Visitor;


/**
 * TODO remove this test. It is identical to Test_075_QueryComplexHolder. 
 * @author Tilmann Zäschke
 *
 */
public class ComplexJdo { //extends JdoDriver implements Complex {
	
	private Object _rootId;
	
	private long _checkSum;
	private int nObjects;
	private int nSelects;
	private int depth;
	
	private PersistenceManager pm;

	@BeforeClass
	public static void beforeClass() {
		TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(ComplexHolder0.class, 
				ComplexHolder1.class,
				ComplexHolder2.class, 
				ComplexHolder3.class, 
				ComplexHolder4.class);
	}
	
	private void commit() {
		pm.currentTransaction().commit();
	}
	
	private void begin() {
		pm.currentTransaction().begin();
	}
	
	private PersistenceManager db() {
		return pm;
	}
	
	private void store(Object pc) {
		pm.makePersistent(pc);
	}
	
	private void delete(Object pc) {
		pm.deletePersistent(pc);
	}
	
	public void addToCheckSum(CheckSummable checkSummable){
		addToCheckSum(checkSummable.checkSum());
	}
    
    /**
     * Collecting a checksum to make sure every team does a complete job  
     */
    private synchronized void addToCheckSum(long l){
        _checkSum += l;
    }
    
	private int objects() {
		return nObjects;
	}
	
	private int selects() {
		return nSelects;
	}
	
	private int depth() {
		return depth;
	}

	private void open() {
		pm = TestTools.openPM();
	}
	
	private void close() {
		TestTools.closePM();
	}
	
	/**
	 * This failed with DataStreamCorrupted exception if makeDirty() was not called when setting
	 * attributes. In that case, the update() failed for most objects.
	 * It is unclear how this corrupted the database.
	 * -> Temporary fix: do not set objects clean in ClientSessionCache.postCommit().
	 */
	@Test
	public void test() {
//		# complex
//		#
//		# [objects]: number of objects to select from
//		# [selects]: number of queries run against all objects
//		complex.objects=1,2,3
//		complex.depth=4,4,4
//		complex.selects=50,50,50
		//non-debug
//		complex.objects=5,6,7
//		complex.depth=6,6,6
//		complex.selects=500,500,500

//		run(1, 1, 4);
//		run(1, 50, 4);
//		run(2, 50, 4);
//		run(3, 50, 4);
		run(5, 500, 6);
		run(6, 500, 6);
		run(7, 500, 6);
	}	

	private void run(int objects, int selects, int depth) {
		nObjects = objects;
		nSelects = selects;
		this.depth = depth;

//		System.out.println("write()");
		open();
		write();
		close();

		open();
//		System.out.println("read()");
		read();
		close();
//
//		open();
////		System.out.println("query()");
//		query();
//		close();
//		
//		open();
////		System.out.println("update()");
//		update();
//		close();
//		
//		open();
////		System.out.println("delete()");
//		delete();
//		close();
	}
    
    //@Override
	public Object write() {
		return write(false);
	}
	
	private Object write(boolean disjunctSpecial) {
		begin();
		ComplexHolder0 holder = ComplexHolder0.generate(depth(), objects(), disjunctSpecial);
		addToCheckSum(holder);
		store(holder);
		_rootId = db().getObjectId(holder);
		commit();
		return _rootId;
	}


	//@Override
	public void read() {
		begin();
		db().getFetchPlan().addGroup("array");
		db().getFetchPlan().addGroup("children");
		db().getFetchPlan().setMaxFetchDepth(-1);
		ComplexHolder0 holder = read(_rootId);
		addToCheckSum(holder);
		db().getFetchPlan().clearGroups();
		commit();
	}
	
	private ComplexHolder0 read(Object id) {
		return (ComplexHolder0) db().getObjectById(id);
	}


	//@Override
	public void query() {
		begin();
		int selectCount = selects();
		int firstInt = objects() * objects() + objects();
		int lastInt = firstInt + (objects() * objects() * objects()) - 1;
		int currentInt = firstInt;
		for (int run = 0; run < selectCount; run++) {
	        String filter = "this.i2 == param";
	        Query query = db().newQuery(db().getExtent(ComplexHolder2.class,true), filter);
	        query.declareParameters("int param");
	        Collection<?> result = (Collection<?>) query.execute(currentInt);
			Iterator<?> it = result.iterator();
			if(! it.hasNext()){
				throw new IllegalStateException("no ComplexHolder2 found");
			}
			ComplexHolder2 holder = (ComplexHolder2) it.next();
			if(it.hasNext()){
				throw new IllegalStateException("more ComplexHolder2 found");
			}
			addToCheckSum(holder.ownCheckSum());
			List<ComplexHolder0> children = holder.getChildren();
			for (ComplexHolder0 child : children) {
				addToCheckSum(child.ownCheckSum());
			}
			ComplexHolder0[] array = holder.getArray();
			for (ComplexHolder0 arrayElement : array) {
				addToCheckSum(arrayElement.ownCheckSum());
			}
			currentInt++;
			if(currentInt > lastInt){
				currentInt = firstInt;
			}
		}
		commit();
	}
	
	//@Override
	public void update() {
		update(_rootId);
	}
	
	private void update(Object id) {
		begin();
		ComplexHolder0 holder = read(id);
		holder.traverse(new NullVisitor<ComplexHolder0>(),
				new Visitor<ComplexHolder0>() {
			@Override
			public void visit(ComplexHolder0 holder) {
				addToCheckSum(holder.ownCheckSum());
				holder.setName("updated");
				List<ComplexHolder0> children = holder.getChildren();
				ComplexHolder0[] array = new ComplexHolder0[children.size()];
				for (int i = 0; i < array.length; i++) {
					array[i] = children.get(i);
				}
				holder.setArray(array);
			}
		});
		commit();
	}
	
	//@Override
	public void delete() {
		deleteById(_rootId);
	}

	private void deleteById(Object id) {
		begin();
		ComplexHolder0 holder = read(id);		
		holder.traverse(
			new NullVisitor<ComplexHolder0>(),
			new Visitor<ComplexHolder0>() {
			@Override
			public void visit(ComplexHolder0 holder) {
				addToCheckSum(holder.ownCheckSum());
				delete(holder);
			}
		});
		commit();
	}
}
