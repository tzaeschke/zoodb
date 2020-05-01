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
package org.zoodb.tools;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import org.zoodb.internal.server.SessionFactory;

/**
 * This class should only be used during development to manage test-runs.
 * 
 * @author Tilmann Zaschke
 *
 */
public class ZooDebug {

	//whether this is a test run
	private static boolean isTesting = false;
	
	private static final ArrayList<FileChannel> fcList = new ArrayList<FileChannel>();
	
	public static boolean isTesting() {
		return isTesting;
	}
	
	public static void setTesting(boolean b) {
		isTesting = b;
	}
	
	public static void registerFile(FileChannel fc) {
		fcList.add(fc);
	}
	
	public static void closeOpenFiles() {
		SessionFactory.clear();
		int failed = 0;
		for (FileChannel fc: fcList) {
			if (fc.isOpen()) {
				failed++;
				try {
					fc.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		fcList.clear();
		if (failed > 0) {
			throw new IllegalStateException("Some files were not closed: " + failed);
		}
	}
}
