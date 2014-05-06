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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.zoodb.internal.Node;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.util.DBLogger;

/**
 * 
 * @author Tilmann Zaeschke
 */
public class SessionFactory {

	private static List<SessionManager> sessions = new ArrayList<>();
	
	public static DiskAccessOneFile getSession(Node node, AbstractCache cache) {
		String dbPath = node.getDbPath();
		DBLogger.debugPrintln(1, "Opening DB file: " + dbPath);

		Path path = FileSystems.getDefault().getPath(dbPath); 

		SessionManager sm = null;
		try {
			//TODO this does not scale
			for (SessionManager smi: sessions) {
				if (Files.isSameFile(smi.getPath(), path)) {
					sm = smi;
					break;
				}
			}
		} catch (IOException e) {
			throw DBLogger.newFatal("Failed while acessing path: " + dbPath, e);
		}

		if (sm == null) {
			//create DB file
			sm = new SessionManager(path);
			sessions.add(sm);
		}
		
		
		return sm.createSession(node, cache);
	}
	
	static void removeSession(SessionManager sm) {
		//TODO this does not scale
		if (!sessions.remove(sm)) {
			throw DBLogger.newFatal("Server session not found for: " + sm.getPath());
		}
	}

	public static void clear() {
		sessions.clear();
	}
}
