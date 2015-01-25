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
package org.zoodb.test.testutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestProcess {

	private final StreamReader streamOut;
	private final StreamReader streamErr;
	private final Process process;

	private TestProcess(Process process) {
		this.process = process;
		this.streamOut = new StreamReader(process.getInputStream(), System.out);
		this.streamErr = new StreamReader(process.getErrorStream(), System.err);
		this.streamOut.start();
		this.streamErr.start();
	}

	
	/**
	 * Stop the process.
	 */
	public void stop() {
		process.destroy();
		streamOut.interrupt();
		streamErr.interrupt();
		try {
			process.waitFor();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Start a process through a main() method.
	 */
	public static TestProcess launchProcess(String optionsAsString, Class<?> mainClass, 
			String[] arguments) {
		String FS = File.separator;
		String jvm = System.getProperty("java.home") + FS + "bin" + FS + "java";
		String cp = System.getProperty("java.class.path");

		List<String> command = new ArrayList<String>();
		command.add(jvm);
		if (optionsAsString != null && optionsAsString.length() > 0) {
			String[] options = optionsAsString.split(" ");
			command.addAll(Arrays.asList(options));
		}
		command.add(mainClass.getName());
		command.addAll(Arrays.asList(arguments));
		
		return createProcess(command, cp);
	}


	/**
	 * Start an RMI server.
	 */
	public static TestProcess launchRMI() {
		String FS = File.separator;
		String cmd = System.getProperty("java.home") + FS + "bin" + FS + "rmiregistry";
		List<String> command = new ArrayList<String>();
		command.add(cmd);
		String cp = System.getProperty("java.class.path");
		return createProcess(command, cp);
	}


	private static TestProcess createProcess(List<String> command, String classPath) {
		ProcessBuilder processBuilder = new ProcessBuilder(command);
		Map< String, String > environment = processBuilder.environment();
		environment.put("CLASSPATH", classPath);
		processBuilder.redirectErrorStream(true);
		try {
			Process prc = processBuilder.start();
			return new TestProcess(prc);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	private static class StreamReader extends Thread {
		
		private final InputStream is;
		private final PrintStream out;
		public StreamReader(InputStream is, PrintStream out) {
			this.is = is;
			this.out = out;
		}
		
		@Override
		public void run() {
			try {
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String line;
				while ((line = br.readLine()) != null) {
					out.println(line);
				}
//				System.out.println("Program terminated!");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}