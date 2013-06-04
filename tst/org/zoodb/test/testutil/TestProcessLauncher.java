/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestProcessLauncher {

	public static Process launchProcess(String optionsAsString, Class<?> mainClass, 
			String[] arguments) {
		try {
			ProcessBuilder processBuilder = createProcess(optionsAsString, mainClass, arguments);
			processBuilder.redirectErrorStream(true);
			Process prc = processBuilder.start();
			
			
			InputStream is = prc.getInputStream();
		    InputStreamReader isr = new InputStreamReader(is);
		    BufferedReader br = new BufferedReader(isr);
		    String line;
		    while ((line = br.readLine()) != null) {
		      System.out.println(line);
		    }
//		    System.out.println("Program terminated!");
			
			return prc;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	private static ProcessBuilder createProcess(String optionsAsString, Class<?> cls, 
			String[] arguments) {
		String jvm = 
			System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
		String cp = System.getProperty("java.class.path");

		String[] options = optionsAsString.split(" ");
		List < String > command = new ArrayList <String>();
		command.add(jvm);
		command.addAll(Arrays.asList(options));
		command.add(cls.getName());
		command.addAll(Arrays.asList(arguments));

		ProcessBuilder processBuilder = new ProcessBuilder(command);
		Map< String, String > environment = processBuilder.environment();
		environment.put("CLASSPATH", cp);
		return processBuilder;
	}
}