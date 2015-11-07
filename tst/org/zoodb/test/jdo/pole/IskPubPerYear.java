package org.zoodb.test.jdo.pole;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class IskPubPerYear {

	private static final int N = 100000;
	private static final int YEARS = 50;
	private static final int YEARS_MIN = 1940;
	
	@BeforeClass
	public static void beforeClass() {
		TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(ComplexHolder0.class, ComplexHolder1.class, ComplexHolder2.class);
		TestTools.defineIndex(ComplexHolder0.class, "id", false);
	}
	
	@Test
	public void testCountPerYear() {
		populate();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		int result = 0;
		
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			for (int i2 = 1945; i2 < 1980; i2+=5) {
				Map<?,?> m = countPerYears(pm, (long)i2-5, (long)i2);
				result += m.size();
			}
		}
		long t1 = System.currentTimeMillis();

		System.out.println("t=" + (t1-t0));
		System.out.println("result=" + result);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	private Map<Long, Long> countPerYears(PersistenceManager pm, Long startYear, Long endYear) {
		Map<Long, Long> resultMap = new HashMap<>();
		Query query = pm.newQuery(ComplexHolder0.class, "id == :year");
		for (long year = startYear; year <= endYear; year++) {
			long number = 0;
			//	            for (Object o: (Collection<?>)query.execute((int)year)) {
			//	            	number++;
			//	            }
			//TODO .size()?!
			number = ((Collection<?>)query.execute((int)year)).size();
			if (number > 0) {
				resultMap.put(year, number );
			}
		}
		return resultMap;
	}

	private void populate() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < N; i++) {
			ComplexHolder1 h = new ComplexHolder1();
			h.setId(YEARS_MIN+i/YEARS);
			pm.makePersistent(h);
			ComplexHolder2 h2 = new ComplexHolder2();
			h2.setId(YEARS_MIN+i/YEARS);
			pm.makePersistent(h2);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}
