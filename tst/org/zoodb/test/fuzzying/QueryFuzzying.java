/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.test.fuzzying;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.zoodb.test.jdo.TestClass;
import org.zoodb.tools.ZooConfig;
import org.zoodb.tools.ZooHelper;

public class QueryFuzzying {

	private static final String[] FILTER = {
		" ", "(", ")", ".", "this", "=", "==", ":", "<", ">", "<=", ">=", "!", "!=",
		"and", "or",
		"&", "|", "^", "~", 
		"0", "1", "9", "0x", "0b", "f", "d", "L", 
		",", "'", "\"", ";", "\\",
		"get", ".get(", "contains", ".contains(", "isEmpty", ".isEmpty",
		"_int", "_bool", "_string", "_ref2",
		"_transInt", "_transString", "_staticInt", "_staticString", "_int", "_long", "_bool", 
		"_char", "_byte", "_short", "_float", "_double", "_bArray", "_intObj", "_string", "_object", "_ref1", "_ref2",
//		};
//
//	private static String[] FILTER2 = {
		"=", "+", "-", "*", "/", 
		"Math", "Math.", "Math.abs", "Math.abs(",
		//"@", "#", "%", "^", 
		"<=", ">=", "!", "!=", 
		"1", ",", "'", "\"", ";"};
	
	
	public static void main(String[] args) {
		int MAX_LEN = 20; 
		int N_TEST = 1000000;
		Random R = new Random(0);
		
		String dbName = "myDB";
		ZooConfig.setFileManager(ZooConfig.FILE_MGR_IN_MEMORY);
		ZooHelper.createDb(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(dbName); 
		pmf.setConnectionURL(dbName);
		PersistenceManager pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		TestClass t = new TestClass();
		pm.makePersistent(t);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		Query q = pm.newQuery(TestClass.class);
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			StringBuilder qs = new StringBuilder(1000);
			int len = R.nextInt(MAX_LEN);
			for (int j = 0; j < len; j++) {
				int p = R.nextInt(FILTER.length);
				String s = FILTER[p];
				qs.append(s);
			}
			String qss = qs.toString();
			int nArg = 0;
			int posArg = 0;
			Map<String, Object> argMap = new HashMap<>();
			while ((posArg = qss.indexOf(":", posArg)) >= 0) {
				posArg++;
				argMap.put("arg"+nArg, nArg);
				nArg++;
			}
			
			
			try {
				q.setFilter(qss);
				if (argMap.isEmpty()) {
					q.execute();
				} else {
					q.executeWithMap(argMap);
				}
				//System.out.println("Success: " + qss);
			} catch (UnsupportedOperationException e) {
				//System.out.println("JdoUE: " + e.getMessage());
			} catch (JDOUserException e) {
				//System.out.println("JdoUE: " + e.getMessage());
			} catch (NumberFormatException e) {
				//System.out.println("NFE:   " + e.getMessage());
			} catch (Throwable e) {
				System.err.println("Checking: " + qss);
				for (String key: argMap.keySet()) {
					System.err.println("   arg: " + key + " -> " + argMap.get(key));
				}
				System.out.println("Queries tested: " + i);
				throw new RuntimeException(e);
			}
		}
		long t1 = System.currentTimeMillis();
		System.out.println("Queries tested: " + N_TEST);
		System.out.println("Time: " + (t1-t0));
	}
	
}
