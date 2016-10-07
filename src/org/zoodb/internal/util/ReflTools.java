/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This class contains tools related to Reflection in java.
 * 
 * 
 * @author Tilmann Zaeschke
 */
public class ReflTools {

    /**
     * Creates a new instance of the class <tt>cls</tt> using the constructor
     * that matches the classes of the given arguments. This method will
     * attempt to use any constructor, regardless of its modifiers. 
     * @param <T>
     * @param cls
     * @param initargs
     * @return a new instance of the class <tt>cls</tt>.
     */
    public static final <T> T newInstance(Class<T> cls, Object ... initargs) {
        Class<?>[] paramCls = null;
        if (initargs != null) {
            paramCls = new Class[initargs.length];
            for (int i = 0; i < initargs.length; i++) {
                paramCls[i] = initargs[i].getClass();
            }
        }
        
        try {
            Constructor<T> con = cls.getDeclaredConstructor(paramCls);
            con.setAccessible(true);
            return con.newInstance(initargs);
        } catch (SecurityException|NoSuchMethodException|IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException|IllegalAccessException|InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Gets the field <tt>fieldName</tt> from class <tt>cls</tt>, makes it
     * accessible with <tt>Field.setAccessible(true)</tt> and returns it.
     * @param <T>
     * @param cls
     * @param fieldName
     * @return the requested <tt>Field</tt> instance.
     */
    public static final <T> Field getField(Class<T> cls, String fieldName) {
        try {
            Field f = cls.getDeclaredField(fieldName);
            f.setAccessible(true);
            return f;
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            if (cls.getSuperclass() != null) {
                return getField(cls.getSuperclass(), fieldName);
            }
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Read the value of the field <tt>fieldName</tt> of object <tt>obj</tt>.
     * @param obj
     * @param fieldName
     * @return the value of the field.
     */
    public static final Object getValue(Object obj, String fieldName) {
        try {
            return getField(obj.getClass(), fieldName).get(obj);
        } catch (IllegalArgumentException|IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Sets the field <tt>fieldName</tt> of an object <tt>obj</tt> to a the
     * value <tt>value</tt>.
     * @param obj
     * @param fieldName
     * @param value
     */
    public static final void setValue(Object obj, String fieldName, 
            Object value) {
        try {
            getField(obj.getClass(), fieldName).set(obj, value);
        } catch (IllegalArgumentException|IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    
    /**
     * Read the value of the field <tt>fieldName</tt> of object <tt>obj</tt>.
     * @param obj
     * @param fieldName
     * @return the value of the field.
     */
    public static int getInt(Object obj, String fieldName) {
        try {
            return getField(obj.getClass(), fieldName).getInt(obj);
        } catch (IllegalArgumentException|IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param obj
     * @param name
     * @param args
     * @return Result.
     */
    public static Object callMethod(Object obj, String name, Object ...args) {
        Class<?>[] argsC = new Class[args.length];
        for (int i =0; i < args.length; i++) {
            if (args[i] == null) {
                argsC[i] = null;
                continue;
            }
            argsC[i] = args.getClass(); 
        }
        
        try {
            Method m = obj.getClass().getDeclaredMethod(name, argsC);
            boolean b = m.isAccessible();
            m.setAccessible(true);
            Object ret = m.invoke(obj, args);
            m.setAccessible(b);
            return ret;
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException|IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException|InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

	public static boolean existsClass(String className) {
		try {
			Class.forName(className);
		} catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> classForName(String className) {
		try {
			return (Class<T>) Class.forName(className);
		} catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
		}
	}

	public static <T> Constructor<T> getConstructor(Class<T> cls, Class<?> ... paramTypes) {
		try {
			return cls.getConstructor(paramTypes);
		} catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
		}
	}

	public static <T> T newInstance(Constructor<T> con, Object ... initArgs) {
		try {
			return con.newInstance(initArgs);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
		}
	}
}
