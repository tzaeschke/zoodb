/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class FileLogger {

	private static final String DB_FILE_NAME = "zooFileLogger.log";
	private static final String DB_REP_PATH = 
		System.getProperty("user.home") + File.separator + "zoodb"; 

	private final PrintWriter out;

	public FileLogger() {
		this(DB_FILE_NAME);
	}

	public FileLogger(String fileName) {
		//create file
		try {
			FileWriter outFile = new FileWriter(DB_REP_PATH + File.separator + fileName);
			out = new PrintWriter(outFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		//prepare closing file
		Runtime.getRuntime().addShutdownHook(new Thread() { 
			public void run() {
				if (out != null) {
					out.flush();
					out.close();
				}
			};
		} );
	}

	private int i = 0;

	public void write(String s) {
		out.append(s);
		if (i++ > 100) {
			out.flush();
			i = 0;
		}
		//		out.flush();
	}

	@Override
	protected void finalize() throws Throwable {
		if (out != null) {
			out.flush();
			out.close();
		}
		//super.finalize();
	}

}
