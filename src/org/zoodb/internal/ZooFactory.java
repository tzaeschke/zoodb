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
			} catch (InstantiationException|IllegalAccessException e) {
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
