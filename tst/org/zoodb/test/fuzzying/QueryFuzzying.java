/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
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

	private static String[] FILTER = {
		" ", "(", ")", ".", "this", "=", "==", ":", "<", ">", "<=", ">=", "!", "!=",
		"and", "or",
		"&", "|", "^", "~", 
		"0", "1", "9", "0x", "0b", "f", "d", "L", 
		",", "'", "\"", ";", "\\",
		"get", ".get(", "contains", ".contains(", "isEmpty", ".isEmpty",
		"_int", "_bool", "_string", "_ref2",
		"_transInt", "_transString", "_staticInt", "_staticString", "_int", "_long", "_bool", 
		"_char", "_byte", "_short", "_float", "_double", "_bArray", "_intObj", "_string", "_object", "_ref1", "_ref2",
		};

	private static String[] FILTER2 = {
		"=", "+", "-", "*", "/", 
		"Math", "Math.", "Math.abs", "Math.abs(",
		//"@", "#", "%", "^", 
		"<=", ">=", "!", "!=", 
		"1", ",", "'", "\"", ";"};
	
	
	public static void main(String[] args) {
		int MAX_LEN = 20; 
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
			} catch (JDOUserException e) {
				//System.out.println("JdoUE: " + e.getMessage());
			} catch (NumberFormatException e) {
				//System.out.println("NFE:   " + e.getMessage());
			} catch (Throwable e) {
				System.err.println("Checking: " + qss);
				for (String key: argMap.keySet()) {
					System.err.println("   arg: " + key + " -> " + argMap.get(key));
				}
				throw new RuntimeException(e);
			}
		}
		
	}
	
}
