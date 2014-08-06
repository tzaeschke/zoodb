package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;

public class ReflectionUtils {
	
	/**
	 * Returns the java field which has the name 'name' in class 'c'
	 * Returns null if no such field exists or the field is inherited.
	 * @param c
	 * @param name
	 * @return
	 */
	public static Field getFieldForName(Class<?> c, String name) {
		Field[] fields = c.getDeclaredFields();
		
		for (int i=0;i<fields.length;i++) {
			if (fields[i].getName().equals(name)) {
				fields[i].setAccessible(true);
				return fields[i];
			}
		}
		return null;
	}

}
