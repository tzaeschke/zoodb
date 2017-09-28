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
package org.zoodb.internal.plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.internal.util.DBLogger;

/**
 * Plug-in initializer for ZooDB plug-ins.
 * 
 * To register a plugin for activation, put a file into the packaged jar.
 * The file goes into: META-INF/services/org.zoodb.spi.ZooPluginActivator
 * The file should contain the class name of an activator class that
 * has a static 'activate()' method.
 * This class and method will be detected by ZooDB and will be executed.
 * The activate() method can then register the plugin. For examples, look at the
 * zoodb-profiler or zoodb-server-btree plugins.   
 * 
 * @author Tilmann Zäschke
 *
 */
public class PluginLoader {

	public static final String SERVICE_RESOURCE_NAME = 
			"META-INF/services/org.zoodb.spi.ZooPluginActivator";

	private static final Logger LOGGER = LoggerFactory.getLogger(PluginLoader.class);
	
	private static final AtomicBoolean hasBeenRun = new AtomicBoolean(false);
	
	private PluginLoader() {
		// private
	}
	
	public static void activatePlugins() {
		
		if (!hasBeenRun.compareAndSet(false, true)) {
			//already activated
			return;
		}
		
		Enumeration<URL> urls; 
		try {
			urls = ClassLoader.getSystemResources(SERVICE_RESOURCE_NAME);
		} catch (IOException e) {
			throw DBLogger.wrap(e);
		}
		
		while (urls.hasMoreElements()) {
			try {
				String className = getClassNameFromURL( (URL) urls.nextElement());
				Class<?> implClass = Class.forName(className);
				Method m = implClass.getMethod("activate");
				m.invoke(null);
				LOGGER.info("Loading plug-in: {}", className);
			} catch (Exception e) {
				throw DBLogger.wrap(e);
			}
		}
	}
	
	private static String getClassNameFromURL (URL url) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0 || line.startsWith("#")) {
                    continue;
                }
                return line.split("\\s")[0];
//                String[] tokens = line.split("\\s");
//                String className = tokens[0];
//                int indexOfComment = className.indexOf('#');
//                if (indexOfComment == -1) {
//                    return className;
//                }
//                return className.substring(0, indexOfComment);
            }
            return null;
        } catch (IOException e) {
			throw DBLogger.wrap(e);
        }
    }

}
