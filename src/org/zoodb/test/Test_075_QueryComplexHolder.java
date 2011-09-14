package org.zoodb.test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.data.CheckSummable;
import org.zoodb.test.data.ComplexHolder0;
import org.zoodb.test.data.ComplexHolder1;
import org.zoodb.test.data.ComplexHolder2;
import org.zoodb.test.data.ComplexHolder3;
import org.zoodb.test.data.ComplexHolder4;
import org.zoodb.test.data.NullVisitor;
import org.zoodb.test.data.Visitor;

public class Test_075_QueryComplexHolder {

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
	 * 
	 * The problem was that the _usedClasses in the DataSerializer were reused for objects on the 
	 * written in the same stream (same page). Randomly accessing these objects meant that the
	 * required used classes were not available, because they were only store with the first object.
	 */
	@Test
	public void test() {
//		run(1, 1, 4);

		run(1, 50, 4);
//		run(2, 50, 4);
//		run(3, 50, 4);
//		run(5, 500, 6);
//		run(6, 500, 6);
//		run(7, 500, 6);
	}	

	protected void run(int objects, int selects, int depth) {
		System.out.println("o=" + objects + "  s=" + selects + " d=" + depth);
		nObjects = objects;
		nSelects = selects;
		this.depth = depth;
		System.out.println("write()");

		open();
		write();
		close();

		open();
		System.out.println("read()");
		read();
		close();

		open();
		System.out.println("query()");
		query();
		close();
		
		open();
		System.out.println("update()");
		update();
		close();
		
		open();
		System.out.println("delete()");
		delete();
		close();
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
	private void read() {
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
	private void query() {
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
				throw new IllegalStateException("no ComplexHolder2 found for i2=" + currentInt);
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
	private void update() {
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
	private void delete() {
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
	
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
}
