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
    
    private ZooDebug() {}

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
