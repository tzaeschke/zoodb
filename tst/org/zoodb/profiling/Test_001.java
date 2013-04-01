package org.zoodb.profiling;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.profiler.test.TestClass;
import org.zoodb.profiler.test.util.TestTools;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldRemovalSuggestion;

public class Test_001 {

	
	@BeforeClass
	public static void beforeCLass() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}
	
	@Before
	public void before() {
		
	}
	
	@After
	public void after() {
		TestTools.closePM();
	}
	
	@Test
	public void test1() {
		List<Long> oids = populate(10);
		
		String tag = "myTag";
		ProfilingManager.getInstance().init(tag);
		
		for (int j = 0; j < 3; j++) {
		
			PersistenceManager pm = TestTools.openPM();
			for (int k = 0; k < 2; k++) {
				pm.currentTransaction().begin();
				
				//navigate
				for (int i = 0; i < 5; i++) {
					TestClass t = (TestClass) pm.getObjectById(oids.get(0));
					while (t != null) {
						t = t.getRef2();
					}
				}
				
				pm.currentTransaction().commit();
			}
			TestTools.closePM();
		}
		
		ProfilingManager.getInstance().finish();
		ActivationArchive aArc = ProfilingManager.getInstance().getPathManager().getArchive(TestClass.class);
		Iterator<AbstractActivation> it = aArc.getIterator();
		while (it.hasNext()) {
			AbstractActivation aa = it.next(); 
			System.out.println("" + aa.getClass().getName() + " -> " + aa.getChildrenCount());
		}
		int nS = 0;
		for (AbstractSuggestion s: ProfilingManager.getInstance().internalGetSuggestions()) {
			System.out.println("name=" + s.getClass().getName() + "  --> " + s.getClazzName());
			if (s instanceof FieldRemovalSuggestion) {
				FieldRemovalSuggestion frs = (FieldRemovalSuggestion) s;
				System.out.println("FieldRemoval: " + frs.getClazzName() + "." + frs.getFieldName());
			}
			nS++;
		}
		System.out.println("Suggestions: " + nS);
		//ProfilingManager.getInstance().save();
	}
	
	private List<Long> populate(int n) {
		List<Long> oids = new ArrayList<Long>(); 
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass t1 = null;
		for (int i = 0; i < n; i++) {
			TestClass t2 = new TestClass();
			t2.setRef2(t1);
			t2.setInt(i+1);
			pm.makePersistent(t2);
			oids.add((Long) pm.getObjectId(t2));
			t1 = t2;
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		return oids;
	}
}
