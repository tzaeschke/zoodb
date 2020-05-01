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
			@Override
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
