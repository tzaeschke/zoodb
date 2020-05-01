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
     * @param <T> The type
     * @param cls The class
     * @param initargs Constructor arguments
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
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Gets the field <tt>fieldName</tt> from class <tt>cls</tt>, makes it
     * accessible with <tt>Field.setAccessible(true)</tt> and returns it.
     * @param <T> The type
     * @param cls The class
     * @param fieldName The field name
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
     * @param obj The object
     * @param fieldName The field name
     * @return the value of the field.
     */
    public static final Object getValue(Object obj, String fieldName) {
        try {
            return getField(obj.getClass(), fieldName).get(obj);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Sets the field <tt>fieldName</tt> of an object <tt>obj</tt> to a the
     * value <tt>value</tt>.
     * @param obj The object
     * @param fieldName the field name
     * @param value The value
     */
    public static final void setValue(Object obj, String fieldName, 
            Object value) {
        try {
            getField(obj.getClass(), fieldName).set(obj, value);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    
    /**
     * Read the value of the field <tt>fieldName</tt> of object <tt>obj</tt>.
     * @param obj The object 
     * @param fieldName The field name
     * @return the value of the field.
     */
    public static int getInt(Object obj, String fieldName) {
        try {
            return getField(obj.getClass(), fieldName).getInt(obj);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param obj The object
     * @param name The method name
     * @param args The method args
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
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
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
