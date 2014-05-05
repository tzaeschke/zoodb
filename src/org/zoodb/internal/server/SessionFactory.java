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
package org.zoodb.internal.server;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.zoodb.internal.Node;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.tools.ZooConfig;

/**
 * 
 * @author Tilmann Zaeschke
 */
public class SessionFactory {

	private static List<SessionInfo> sessions = new ArrayList<>();
	
	private static class SessionInfo {
		private final FreeSpaceManager fsm;
		private final Path path;
		int count = 1;
		public SessionInfo(FreeSpaceManager fsm, Path path) {
			this.path = path;
			this.fsm = fsm;
		}
	}
	
	public static DiskAccessOneFile getSession(Node node, AbstractCache cache) {
		String dbPath = node.getDbPath();
		DBLogger.debugPrintln(1, "Opening DB file: " + dbPath);

		Path path = FileSystems.getDefault().getPath(dbPath); 
		
		FreeSpaceManager fsm = null;
		StorageChannel file = null;
		try {
			//TODO this does not scale
			for (SessionInfo si: sessions) {
				if (Files.isSameFile(si.path, path)) {
					fsm = si.fsm;
					file = fsm.getFile();
					si.count++;
					break;
				}
			}
		} catch (IOException e) {
			throw DBLogger.newFatal("Failed while acessing path: " + dbPath, e);
		}

		if (fsm == null) {
			//create DB file
			fsm = new FreeSpaceManager();
			file = createPageAccessFile(dbPath, "rw", fsm);
			sessions.add(new SessionInfo(fsm, path));
		}
		
		
		return new DiskAccessOneFile(node, cache, fsm, file);
	}
	
	private static StorageChannel createPageAccessFile(String dbPath, String options, 
			FreeSpaceManager fsm) {
		try {
			Class<?> cls = Class.forName(ZooConfig.getFileProcessor());
			Constructor<?> con = cls.getConstructor(String.class, String.class, Integer.TYPE, 
					FreeSpaceManager.class);
			StorageChannel paf = 
				(StorageChannel) con.newInstance(dbPath, options, ZooConfig.getFilePageSize(), fsm);
			return paf;
		} catch (Exception e) {
			if (e instanceof InvocationTargetException) {
				Throwable t2 = e.getCause();
				if (DBLogger.USER_EXCEPTION.isAssignableFrom(t2.getClass())) {
					throw (RuntimeException)t2;
				}
			}
			throw DBLogger.newFatal("path=" + dbPath, e);
		}
	}
	
	static void endSession(FreeSpaceManager fsm) {
		//TODO this does not scale
		for (SessionInfo si: sessions) {
			if (fsm == si.fsm) {
				si.count--;
				if (si.count == 0) {
					DBLogger.debugPrintln(1, "Closing DB file: " + si.path);
					fsm.getFile().close();
					//TODO this does not scale
					sessions.remove(si);
				}
				return;
			}
		}

		throw DBLogger.newFatal("Server session not found!");
	}

	public static void clear() {
		sessions.clear();
	}
}
