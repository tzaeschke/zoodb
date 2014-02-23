/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.java;

import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Random;

import javax.jdo.spi.PersistenceCapable;

import org.zoodb.api.impl.ZooPCImpl;


public class PerfNativeCall {

	private final Class<?>[] CLASSES = {
			Long.TYPE, String.class, Date.class, Integer.class, ZooPCImpl.class, 
			PersistenceCapable.class, Byte.class, Byte.TYPE, Integer.TYPE, Class.class};

	private enum PRIMITIVE {
		/** BOOL */ BOOLEAN, 
		/** BYTE */ BYTE, 
		/** CHAR */ CHAR, 
		/** DOUBLE */ DOUBLE, 
		/** FLOAT */ FLOAT, 
		/** INT */ INT, 
		/** LONG */ LONG,
		/** SHORT */ SHORT}
	private static final IdentityHashMap<Class<?>, PRIMITIVE> PRIMITIVE_CLASSES = 
		new IdentityHashMap<Class<?>, PRIMITIVE>();
	static {
		PRIMITIVE_CLASSES.put(Boolean.class, PRIMITIVE.BOOLEAN);
		PRIMITIVE_CLASSES.put(Byte.class, PRIMITIVE.BYTE);
		PRIMITIVE_CLASSES.put(Character.class, PRIMITIVE.CHAR);
		PRIMITIVE_CLASSES.put(Double.class, PRIMITIVE.DOUBLE);
		PRIMITIVE_CLASSES.put(Float.class, PRIMITIVE.FLOAT);
		PRIMITIVE_CLASSES.put(Integer.class, PRIMITIVE.INT);
		PRIMITIVE_CLASSES.put(Long.class, PRIMITIVE.LONG);
		PRIMITIVE_CLASSES.put(Short.class, PRIMITIVE.SHORT);
	}

	private static final IdentityHashMap<Class<?>, PRIMITIVE> PRIMITIVE_TYPES = 
		new IdentityHashMap<Class<?>, PRIMITIVE>();
	static {
		PRIMITIVE_TYPES.put(Boolean.TYPE, PRIMITIVE.BOOLEAN);
		PRIMITIVE_TYPES.put(Byte.TYPE, PRIMITIVE.BYTE);
		PRIMITIVE_TYPES.put(Character.TYPE, PRIMITIVE.CHAR);
		PRIMITIVE_TYPES.put(Double.TYPE, PRIMITIVE.DOUBLE);
		PRIMITIVE_TYPES.put(Float.TYPE, PRIMITIVE.FLOAT);
		PRIMITIVE_TYPES.put(Integer.TYPE, PRIMITIVE.INT);
		PRIMITIVE_TYPES.put(Long.TYPE, PRIMITIVE.LONG);
		PRIMITIVE_TYPES.put(Short.TYPE, PRIMITIVE.SHORT);
	}


	private long timer;

	private static final int N = 1000000;

	public static void main(String[] args) {
		new PerfNativeCall().run();
	}

	public void run() {
		Class<?>[] classes = new Class[N];
		Random rnd = new Random();
		for (int i = 0; i < N; i++) {
			int i2 = Math.abs(rnd.nextInt()) % 10;
			classes[i] = CLASSES[i2];
		}

		long n = 0;
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n = classes[j].isPrimitive() ? n++ : n--;
			}
			for (int j = 0; j < N; j++) {
				n = PRIMITIVE_TYPES.containsKey(classes[j]) ? n++ : n--;
			}
		}

		start();
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n = classes[j].isPrimitive() ? n++ : n--;
			}
		}
		stop("isPrimitive()");

		start();
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n = PRIMITIVE_TYPES.containsKey(classes[j]) ? n++ : n--;
			}
		}
		stop("IdentityHashMap");

		System.out.println(n);
	}


	private void start() {
		timer = System.currentTimeMillis();
	}

	private void stop(String str) {
		long t = System.currentTimeMillis() - timer;
		System.out.println(str + ": " + t);
	}
}
