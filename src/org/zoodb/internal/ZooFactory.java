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
package org.zoodb.internal;

import org.zoodb.tools.ZooConfig;


/**
 * 
 * @author ztilmann
 *
 */
public abstract class ZooFactory {

	private static ZooFactory f;
	
	public static ZooFactory get() {
		if (f == null) {
			try {
				f = (ZooFactory) findClass("Factory").newInstance();
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} 
		}
		return f;
	}
	
    private static Class<?> findClass(String clsName) {
		try {
			String root = "org.zoodb.internal.model";
			if (ZooConfig.MODEL == ZooConfig.MODEL_1P) {
				return Class.forName(root + "1p." + clsName + "1P");
			} else if (ZooConfig.MODEL == ZooConfig.MODEL_2P) {
				return Class.forName(root + "2p." + clsName + "2P"); 
			}
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} 
		throw new IllegalStateException("Model = " + ZooConfig.MODEL);
	}
	
	public abstract Node createNode(String dbPath, Session session);

}
