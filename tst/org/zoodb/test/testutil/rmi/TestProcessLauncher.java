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
package org.zoodb.test.testutil.rmi;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.zoodb.internal.util.DBLogger;

public class TestProcessLauncher implements Runnable {

	private final Process process;

	private TestProcessLauncher(Process process) {
		this.process = process;
	}

	public static Process launchProcess(String optionsAsString, Class<?> mainClass, 
			String[] arguments) {
		try {
			ProcessBuilder processBuilder = createProcess(optionsAsString, mainClass, arguments);
			processBuilder.redirectErrorStream(true);
			Process prc = processBuilder.start();
			TestProcessLauncher l = new TestProcessLauncher(prc);

			//output monitor thread
			new Thread(l).start();

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
		List<String> command = new ArrayList <>();
		command.add(jvm);
		command.addAll(Arrays.asList(options));
		command.add(cls.getName());
		command.addAll(Arrays.asList(arguments));

		ProcessBuilder processBuilder = new ProcessBuilder(command);
		Map< String, String > environment = processBuilder.environment();
		environment.put("CLASSPATH", cp);
		return processBuilder;
	}



	@Override
	public void run() {
		try {
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
			System.out.println("Program terminated!");
		} catch (IOException e) {
			throw DBLogger.wrap(e);
		}
	}
}