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
package org.zoodb.internal.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.internal.Node;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.plugin.PluginLoader;
import org.zoodb.internal.util.DBLogger;

/**
 * 
 * @author Tilmann Zaeschke
 */
public class SessionFactory {

	public static final Logger LOGGER = LoggerFactory.getLogger(SessionFactory.class);

	/**
	 * This is a hack to ensure that we don't use non-transactional read with multiple sessions.
	 */
	//TODO remove the following two!
	@Deprecated
	public static boolean FAIL_BECAUSE_OF_ACTIVE_NON_TX_READ = false;
	//TODO remove the following!
	@Deprecated
	public static boolean MULTIPLE_SESSIONS_ARE_OPEN = false;
	
	private static final List<SessionManager> sessions = new ArrayList<>();
	
	static {
		PluginLoader.activatePlugins();
	}

	public static DiskAccessOneFile getSession(Node node, AbstractCache cache) {
		String dbPath = node.getDbPath();
		LOGGER.info("Opening DB file: {}", dbPath);

		Path path = FileSystems.getDefault().getPath(dbPath); 

		SessionManager sm = null;
		synchronized (sessions) {
			try {
				//TODO this does not scale
				for (SessionManager smi: sessions) {
					if (Files.isSameFile(smi.getPath(), path)) {
						sm = smi;
						break;
					}
				}
			} catch (IOException e) {
				throw DBLogger.newFatal("Failed while accessing path: " + dbPath, e);
			}

			if (sm == null) {
				//create DB file
				sm = new SessionManager(path);
				sessions.add(sm);
			} else {
				MULTIPLE_SESSIONS_ARE_OPEN = true;
				if (FAIL_BECAUSE_OF_ACTIVE_NON_TX_READ) {
					throw DBLogger.newFatal("Not supported: Can't use non-transactional read with "
							+ "mutliple sessions");
				}
			}
			//The following needs to be synchronized because it requests read/write channels.
			//The channel-list may be accessed concurrently from this.removeSession().
			//Also, simply making the List synchronized resulted in occasional deadlocks.
			return sm.createSession(node, cache);
		}
	}
	
	static void removeSession(SessionManager sm) {
		synchronized (sessions) {
			//TODO this does not scale
			if (!sessions.remove(sm)) {
				throw DBLogger.newFatalInternal("Server session not found for: " + sm.getPath());
			}
			if (sessions.size() <= 1) {
				MULTIPLE_SESSIONS_ARE_OPEN = false;
			}
			if (sessions.isEmpty()) {
				FAIL_BECAUSE_OF_ACTIVE_NON_TX_READ = false;
			}
		}
	}

	public static void clear() {
		synchronized (sessions) {
			sessions.clear();
			FAIL_BECAUSE_OF_ACTIVE_NON_TX_READ = false;
			MULTIPLE_SESSIONS_ARE_OPEN = false;
		}
	}

	public static void cleanUp(File dbFile) {
		Path path = dbFile.toPath(); 

		try {
			synchronized (sessions) {
				//TODO this does not scale
				for (SessionManager smi: sessions) {
					if (Files.isSameFile(smi.getPath(), path)) {
						if (smi.isLocked()) {
							throw DBLogger.newUser("Found open session on " + dbFile);
						}
						sessions.remove(smi);
						break;
					}
				}
				FAIL_BECAUSE_OF_ACTIVE_NON_TX_READ = false;
				MULTIPLE_SESSIONS_ARE_OPEN = false;
			}
		} catch (IOException e) {
			throw DBLogger.newFatal("Failed while accessing path: " + dbFile, e);
		}
	}
	
	public static FileHeader readHeader(Path path) {
		return SessionManager.readHeader(path);
	}
}
